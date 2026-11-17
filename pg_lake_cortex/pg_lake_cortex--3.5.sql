/* pg_lake_cortex--3.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake_cortex" to load this file. \quit

/*
 * pg_lake_cortex exposes the Snowflake Cortex REST API as Postgres functions.
 * Every call becomes one request to the account behind a pg_lake_snowflake
 * foreign server, made with the credentials of the user mapping that applies
 * to the caller. A request runs no SQL and needs no warehouse; the account
 * bills the tokens the model processes.
 *
 * The server is the server_name argument when given, and otherwise the
 * pg_lake_cortex.default_server setting.
 */
CREATE SCHEMA cortex;
GRANT USAGE ON SCHEMA cortex TO lake_read;

/* the server a call goes to */
CREATE FUNCTION cortex.resolve_server(server_name text)
 RETURNS text
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
  server text := coalesce(server_name,
                          nullif(current_setting('pg_lake_cortex.default_server', true), ''));
BEGIN
  IF server IS NULL THEN
    RAISE EXCEPTION 'no Snowflake server to run Cortex functions on'
      USING HINT = 'Pass server_name, or set pg_lake_cortex.default_server to the name of a pg_lake_snowflake foreign server.';
  END IF;

  RETURN server;
END;
$function$;

/*
 * cortex.request posts a body to any Cortex REST endpoint and returns the
 * answer, for what the functions below do not cover.
 */
CREATE FUNCTION cortex.request(path text, body jsonb, server_name text DEFAULT NULL)
 RETURNS jsonb
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT lake_snowflake.cortex_request(cortex.resolve_server(server_name), path, body)
$function$;

COMMENT ON FUNCTION cortex.request(text,jsonb,text) IS 'post a request to a Cortex REST API endpoint (a path below /api/v2/cortex/) and return the JSON answer';

/*
 * cortex.chat_request builds a chat completions request for one user message.
 *
 * model_parameters takes the options of AI_COMPLETE. The REST API calls the
 * output limit max_completion_tokens and refuses max_tokens, so that one is
 * renamed; the others (temperature, top_p) are passed on as they are.
 */
CREATE FUNCTION cortex.chat_request(model text, prompt text, model_parameters jsonb,
                                    response_format jsonb DEFAULT NULL)
 RETURNS jsonb
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT jsonb_strip_nulls(jsonb_build_object(
           'model', model,
           'messages', jsonb_build_array(jsonb_build_object('role', 'user', 'content', prompt)),
           'response_format', response_format))
         || coalesce(model_parameters - 'max_tokens', '{}')
         || CASE WHEN model_parameters ? 'max_tokens'
                 THEN jsonb_build_object('max_completion_tokens', model_parameters -> 'max_tokens')
                 ELSE '{}' END
$function$;

/*
 * cortex.chat_response_format turns the response_format of AI_COMPLETE,
 * {"type": "json", "schema": ...}, into the json_schema form of the chat
 * completions API. A response_format already in that form is left alone.
 */
CREATE FUNCTION cortex.chat_response_format(response_format jsonb)
 RETURNS jsonb
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT CASE WHEN response_format ->> 'type' = 'json' AND response_format ? 'schema'
              THEN jsonb_build_object('type', 'json_schema',
                                      'json_schema', jsonb_build_object('name', 'response',
                                                                        'schema', response_format -> 'schema'))
              ELSE response_format END
$function$;

/*
 * The Cortex functions. A NULL input returns NULL without a request, and they
 * are VOLATILE because each one is a remote call whose answer can change.
 */

CREATE FUNCTION cortex.complete(model text, prompt text,
                                model_parameters jsonb DEFAULT NULL,
                                server_name text DEFAULT NULL)
 RETURNS text
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN model IS NULL OR prompt IS NULL THEN NULL ELSE
    cortex.request('/api/v2/cortex/v1/chat/completions',
                   cortex.chat_request(model, prompt, model_parameters),
                   server_name) #>> '{choices,0,message,content}'
  END
$function$;

COMMENT ON FUNCTION cortex.complete(text,text,jsonb,text) IS 'generate a completion for a prompt with a Cortex model (chat completions API)';

CREATE FUNCTION cortex.complete_json(model text, prompt text,
                                     response_format jsonb,
                                     model_parameters jsonb DEFAULT NULL,
                                     server_name text DEFAULT NULL)
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE
AS $function$
DECLARE
  content text;
BEGIN
  IF model IS NULL OR prompt IS NULL OR response_format IS NULL THEN
    RETURN NULL;
  END IF;

  content := cortex.request('/api/v2/cortex/v1/chat/completions',
                            cortex.chat_request(model, prompt, model_parameters,
                                                cortex.chat_response_format(response_format)),
                            server_name) #>> '{choices,0,message,content}';

  BEGIN
    RETURN content::jsonb;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'Cortex model % answered with something other than JSON', model
      USING DETAIL = 'The answer starts with: ' || left(content, 200);
  END;
END;
$function$;

COMMENT ON FUNCTION cortex.complete_json(text,text,jsonb,jsonb,text) IS 'generate a completion that follows a JSON schema (chat completions API with response_format)';

/*
 * cortex.embed_batch embeds many texts in one request, which is what makes
 * filling an embedding column affordable: the answer for inputs[i] comes back
 * with index i.
 */
CREATE FUNCTION cortex.embed_batch(model text, inputs text[], server_name text DEFAULT NULL)
 RETURNS TABLE (index int, embedding float8[])
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT (entry ->> 'index')::int + 1,
         ARRAY(SELECT element::float8
               FROM jsonb_array_elements_text(entry #> '{embedding,0}') AS element)
  FROM jsonb_array_elements(
         cortex.request('/api/v2/cortex/inference:embed',
                        jsonb_build_object('model', model, 'text', to_jsonb(inputs)),
                        server_name) -> 'data') AS entry
  WHERE model IS NOT NULL AND inputs IS NOT NULL AND cardinality(inputs) > 0
$function$;

COMMENT ON FUNCTION cortex.embed_batch(text,text[],text) IS 'embed many texts as vectors in one request; index is the position in inputs';

CREATE FUNCTION cortex.embed(model text, input text, server_name text DEFAULT NULL)
 RETURNS float8[]
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN model IS NULL OR input IS NULL THEN NULL ELSE
    (SELECT embedding FROM cortex.embed_batch(model, ARRAY[input], server_name))
  END
$function$;

COMMENT ON FUNCTION cortex.embed(text,text,text) IS 'embed a text as a vector with a Cortex embedding model';

/*
 * cortex.similarity embeds both texts in one request and returns the cosine
 * similarity of the two vectors, from -1 to 1.
 */
CREATE FUNCTION cortex.similarity(input1 text, input2 text,
                                  model text DEFAULT 'snowflake-arctic-embed-m-v1.5',
                                  server_name text DEFAULT NULL)
 RETURNS float8
 LANGUAGE SQL
 VOLATILE
AS $function$
  SELECT CASE WHEN input1 IS NULL OR input2 IS NULL OR model IS NULL THEN NULL ELSE
    (SELECT sum(a.value * b.value) /
            nullif(sqrt(sum(a.value * a.value)) * sqrt(sum(b.value * b.value)), 0)
     FROM (SELECT array_agg(embedding ORDER BY index) AS vectors
           FROM cortex.embed_batch(model, ARRAY[input1, input2], server_name)) e,
          unnest(e.vectors[1:1]) WITH ORDINALITY AS a(value, position)
          JOIN unnest(e.vectors[2:2]) WITH ORDINALITY AS b(value, position)
          USING (position))
  END
$function$;

COMMENT ON FUNCTION cortex.similarity(text,text,text,text) IS 'cosine similarity of the embeddings of two texts, from -1 to 1';

/*
 * The functions can only reach an account through a server the caller has
 * USAGE on and a user mapping for, so they are granted like
 * lake_snowflake.cortex_request is.
 */
DO $do$
DECLARE
  f regprocedure;
BEGIN
  FOR f IN
    SELECT p.oid::regprocedure FROM pg_proc p
    WHERE p.pronamespace = 'cortex'::regnamespace
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM public', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO lake_read', f);
  END LOOP;
END;
$do$;
