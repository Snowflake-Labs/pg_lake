/* pg_lake_cortex--3.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake_cortex" to load this file. \quit

/*
 * pg_lake_cortex exposes the Snowflake Cortex AI functions as Postgres
 * functions. Every call becomes one statement on the account behind a
 * pg_lake_snowflake foreign server, run with the credentials of the user
 * mapping that applies to the caller, and on that account's warehouse.
 *
 * The server is the server_name argument when given, and otherwise the
 * lake_cortex.default_server setting.
 */
CREATE SCHEMA lake_cortex;
GRANT USAGE ON SCHEMA lake_cortex TO lake_read;

/*
 * lake_cortex.sql_literal turns a value into a Snowflake string literal.
 *
 * Unlike a standard-conforming Postgres string, a Snowflake string treats a
 * backslash as an escape character, so backslashes are doubled along with
 * quotes. Cortex functions require some arguments (the model above all) to be
 * literals rather than expressions, which is why arguments are inlined rather
 * than passed as one parsed JSON document.
 */
CREATE FUNCTION lake_cortex.sql_literal(value text)
 RETURNS text
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT CASE WHEN value IS NULL THEN 'NULL'
         ELSE '''' || replace(replace(value, '\', '\\'), '''', '''''') || ''''
         END
$function$;

/* a JSON value as a Snowflake VARIANT expression */
CREATE FUNCTION lake_cortex.sql_json(value jsonb)
 RETURNS text
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT CASE WHEN value IS NULL THEN 'NULL'
         ELSE 'PARSE_JSON(' || lake_cortex.sql_literal(value::text) || ')'
         END
$function$;

/*
 * lake_cortex.invoke evaluates a Snowflake expression on the account and
 * returns its value as JSON. Going through TO_JSON keeps every result type,
 * including OBJECT, ARRAY and VECTOR, in one wire form that jsonb can parse.
 */
CREATE FUNCTION lake_cortex.invoke(expression text, server_name text DEFAULT NULL)
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE
AS $function$
DECLARE
  server text := coalesce(server_name,
                          nullif(current_setting('lake_cortex.default_server', true), ''));
  result text;
BEGIN
  IF server IS NULL THEN
    RAISE EXCEPTION 'no Snowflake server to run Cortex functions on'
      USING HINT = 'Pass server_name, or set lake_cortex.default_server to the name of a pg_lake_snowflake foreign server.';
  END IF;

  result := lake_snowflake.execute(server,
                                   'SELECT TO_JSON(TO_VARIANT(' || expression || '))');

  RETURN result::jsonb;
END;
$function$;

COMMENT ON FUNCTION lake_cortex.invoke(text,text) IS 'evaluate a Snowflake expression on a Snowflake account and return its value as jsonb';
REVOKE ALL ON FUNCTION lake_cortex.invoke(text,text) FROM public;
GRANT EXECUTE ON FUNCTION lake_cortex.invoke(text,text) TO lake_read;

/*
 * The Cortex functions. They are STRICT where Cortex would answer NULL for a
 * NULL input anyway, which saves the round trip, and VOLATILE because each one
 * is a remote call whose answer can change.
 */

CREATE FUNCTION lake_cortex.complete(model text, prompt text,
                                     model_parameters jsonb DEFAULT NULL,
                                     server_name text DEFAULT NULL)
 RETURNS text
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN model IS NULL OR prompt IS NULL THEN NULL ELSE
    lake_cortex.invoke(
      'AI_COMPLETE(model => ' || lake_cortex.sql_literal(model) ||
      ', prompt => ' || lake_cortex.sql_literal(prompt) ||
      CASE WHEN model_parameters IS NULL THEN ''
           ELSE ', model_parameters => ' || lake_cortex.sql_json(model_parameters) END ||
      ')', server_name) #>> '{}'
  END
$function$;

COMMENT ON FUNCTION lake_cortex.complete(text,text,jsonb,text) IS 'generate a completion for a prompt with a Cortex model (AI_COMPLETE)';

CREATE FUNCTION lake_cortex.complete_json(model text, prompt text,
                                          response_format jsonb,
                                          model_parameters jsonb DEFAULT NULL,
                                          server_name text DEFAULT NULL)
 RETURNS jsonb
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN model IS NULL OR prompt IS NULL OR response_format IS NULL THEN NULL ELSE
    lake_cortex.invoke(
      'AI_COMPLETE(model => ' || lake_cortex.sql_literal(model) ||
      ', prompt => ' || lake_cortex.sql_literal(prompt) ||
      ', model_parameters => ' || coalesce(lake_cortex.sql_json(model_parameters), 'NULL') ||
      ', response_format => ' || lake_cortex.sql_json(response_format) ||
      ')', server_name)
  END
$function$;

COMMENT ON FUNCTION lake_cortex.complete_json(text,text,jsonb,jsonb,text) IS 'generate a completion that follows a JSON schema (AI_COMPLETE with response_format)';

CREATE FUNCTION lake_cortex.classify(input text, categories text[],
                                     server_name text DEFAULT NULL)
 RETURNS text[]
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input IS NULL OR categories IS NULL THEN NULL ELSE
    ARRAY(SELECT jsonb_array_elements_text(
      lake_cortex.invoke(
        'AI_CLASSIFY(' || lake_cortex.sql_literal(input) || ', ' ||
        lake_cortex.sql_json(to_jsonb(categories)) || ')', server_name) -> 'labels'))
  END
$function$;

COMMENT ON FUNCTION lake_cortex.classify(text,text[],text) IS 'pick the categories that apply to a text (AI_CLASSIFY)';

CREATE FUNCTION lake_cortex.filter(predicate text, input text,
                                   server_name text DEFAULT NULL)
 RETURNS boolean
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN predicate IS NULL OR input IS NULL THEN NULL ELSE
    (lake_cortex.invoke(
      'AI_FILTER(' || lake_cortex.sql_literal(predicate || ' ' || input) || ')',
      server_name) #>> '{}')::boolean
  END
$function$;

COMMENT ON FUNCTION lake_cortex.filter(text,text,text) IS 'answer a yes/no question about a text (AI_FILTER)';

CREATE FUNCTION lake_cortex.sentiment(input text, server_name text DEFAULT NULL)
 RETURNS text
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input IS NULL THEN NULL ELSE
    (SELECT category ->> 'sentiment'
     FROM jsonb_array_elements(
       lake_cortex.invoke('AI_SENTIMENT(' || lake_cortex.sql_literal(input) || ')',
                          server_name) -> 'categories') AS category
     WHERE category ->> 'name' = 'overall')
  END
$function$;

COMMENT ON FUNCTION lake_cortex.sentiment(text,text) IS 'overall sentiment of a text: positive, negative, neutral or mixed (AI_SENTIMENT)';

CREATE FUNCTION lake_cortex.translate(input text, source_language text,
                                      target_language text,
                                      server_name text DEFAULT NULL)
 RETURNS text
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input IS NULL OR target_language IS NULL THEN NULL ELSE
    lake_cortex.invoke(
      'AI_TRANSLATE(' || lake_cortex.sql_literal(input) || ', ' ||
      lake_cortex.sql_literal(coalesce(source_language, '')) || ', ' ||
      lake_cortex.sql_literal(target_language) || ')', server_name) #>> '{}'
  END
$function$;

COMMENT ON FUNCTION lake_cortex.translate(text,text,text,text) IS 'translate a text; a NULL or empty source language is detected (AI_TRANSLATE)';

CREATE FUNCTION lake_cortex.summarize(input text, server_name text DEFAULT NULL)
 RETURNS text
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input IS NULL THEN NULL ELSE
    lake_cortex.invoke(
      'SNOWFLAKE.CORTEX.SUMMARIZE(' || lake_cortex.sql_literal(input) || ')',
      server_name) #>> '{}'
  END
$function$;

COMMENT ON FUNCTION lake_cortex.summarize(text,text) IS 'summarize a text (SNOWFLAKE.CORTEX.SUMMARIZE)';

CREATE FUNCTION lake_cortex.extract(input text, response_format jsonb,
                                    server_name text DEFAULT NULL)
 RETURNS jsonb
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input IS NULL OR response_format IS NULL THEN NULL ELSE
    lake_cortex.invoke(
      'AI_EXTRACT(text => ' || lake_cortex.sql_literal(input) ||
      ', responseFormat => ' || lake_cortex.sql_json(response_format) || ')',
      server_name) -> 'response'
  END
$function$;

COMMENT ON FUNCTION lake_cortex.extract(text,jsonb,text) IS 'extract the answers to questions about a text (AI_EXTRACT)';

CREATE FUNCTION lake_cortex.redact(input text, server_name text DEFAULT NULL)
 RETURNS text
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input IS NULL THEN NULL ELSE
    lake_cortex.invoke('AI_REDACT(' || lake_cortex.sql_literal(input) || ')',
                       server_name) #>> '{}'
  END
$function$;

COMMENT ON FUNCTION lake_cortex.redact(text,text) IS 'replace personal information in a text with placeholders (AI_REDACT)';

CREATE FUNCTION lake_cortex.embed(model text, input text, server_name text DEFAULT NULL)
 RETURNS float8[]
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN model IS NULL OR input IS NULL THEN NULL ELSE
    ARRAY(SELECT element::float8
          FROM jsonb_array_elements_text(
            lake_cortex.invoke(
              'AI_EMBED(' || lake_cortex.sql_literal(model) || ', ' ||
              lake_cortex.sql_literal(input) || ')::ARRAY', server_name)) AS element)
  END
$function$;

COMMENT ON FUNCTION lake_cortex.embed(text,text,text) IS 'embed a text as a vector with a Cortex embedding model (AI_EMBED)';

CREATE FUNCTION lake_cortex.similarity(input1 text, input2 text,
                                       server_name text DEFAULT NULL)
 RETURNS float8
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input1 IS NULL OR input2 IS NULL THEN NULL ELSE
    (lake_cortex.invoke(
      'AI_SIMILARITY(' || lake_cortex.sql_literal(input1) || ', ' ||
      lake_cortex.sql_literal(input2) || ')', server_name) #>> '{}')::float8
  END
$function$;

COMMENT ON FUNCTION lake_cortex.similarity(text,text,text) IS 'semantic similarity of two texts, from -1 to 1 (AI_SIMILARITY)';

CREATE FUNCTION lake_cortex.count_tokens(function_name text, model text, input text,
                                         server_name text DEFAULT NULL)
 RETURNS bigint
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN function_name IS NULL OR input IS NULL THEN NULL ELSE
    (lake_cortex.invoke(
      'AI_COUNT_TOKENS(' || lake_cortex.sql_literal(function_name) || ', ' ||
      CASE WHEN model IS NULL THEN '' ELSE lake_cortex.sql_literal(model) || ', ' END ||
      lake_cortex.sql_literal(input) || ')', server_name) #>> '{}')::bigint
  END
$function$;

COMMENT ON FUNCTION lake_cortex.count_tokens(text,text,text,text) IS 'number of tokens a Cortex function would bill for an input (AI_COUNT_TOKENS)';

/*
 * The functions can only reach an account through a server the caller has
 * USAGE on and a user mapping for, so they are granted like
 * lake_snowflake.execute is.
 */
DO $do$
DECLARE
  f regprocedure;
BEGIN
  FOR f IN
    SELECT p.oid::regprocedure FROM pg_proc p
    WHERE p.pronamespace = 'lake_cortex'::regnamespace
      AND p.proname NOT IN ('sql_literal', 'sql_json', 'invoke')
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM public', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO lake_read', f);
  END LOOP;
END;
$do$;
