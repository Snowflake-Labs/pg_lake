/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/parquet/geoparquet.h"


/*
 * GeometryColumn represents a specific geometry column in a table.
 */
typedef struct GeometryColumn
{
	/* name of the column */
	char	   *name;

	/* geometry type (point, linestring, ...), see postgis.h */
	int			geometryType;

} GeometryColumn;


static List *GetGeometryColumns(TupleDesc tupleDesc);
static char *GetGeometryTypeName(int geometryType);


/*
 * GetGeoParquetMetadataForTupleDesc constructs GeoParquet metadata if
 * any of the columns are geometry type.
 */
char *
GetGeoParquetMetadataForTupleDesc(TupleDesc tupleDesc)
{
	List	   *geometryColumns = GetGeometryColumns(tupleDesc);

	if (geometryColumns == NIL)
		return NULL;

	StringInfoData geoParquetMetadata;

	initStringInfo(&geoParquetMetadata);

	/* open geoparquet metadata */
	appendStringInfoString(&geoParquetMetadata,
						   "{\"version\":\"1.1.0\"");

	ListCell   *columnCell = NULL;
	bool		isFirst = true;

	foreach(columnCell, geometryColumns)
	{
		GeometryColumn *column = lfirst(columnCell);

		/* we currently treat the first geometry column as primary */
		if (isFirst)
		{
			appendStringInfo(&geoParquetMetadata,
							 ", \"primary_column\":%s, \"columns\":{",
							 EscapeJson(column->name));

			isFirst = false;
		}
		else
		{
			appendStringInfoString(&geoParquetMetadata, ",");
		}

		/* open column description */
		appendStringInfo(&geoParquetMetadata, "%s:{",
						 EscapeJson(column->name));

		/* we currently always write using WKB encoding */
		appendStringInfoString(&geoParquetMetadata, "\"encoding\":\"WKB\"");

		/* open geometry_types array */
		appendStringInfoString(&geoParquetMetadata, ", \"geometry_types\":[");

		char	   *geometryTypeName = GetGeometryTypeName(column->geometryType);

		/* only simple geometry types are defined in the GeoParquet spec */
		if (geometryTypeName != NULL)
			appendStringInfoString(&geoParquetMetadata, EscapeJson(geometryTypeName));

		/* close geometry_types */
		appendStringInfoString(&geoParquetMetadata, "]");

		/* close column description */
		appendStringInfoString(&geoParquetMetadata, "}");
	}

	/* close columns map (opened by for loop) */
	appendStringInfoString(&geoParquetMetadata, "}");

	/* close geoparquet metadata */
	appendStringInfoString(&geoParquetMetadata, "}");

	return geoParquetMetadata.data;
}


/*
 * GetGeometryColumns returns metadata about geometry columns.
 */
static List *
GetGeometryColumns(TupleDesc tupleDesc)
{
	List	   *geometryColumns = NIL;

	for (int columnIndex = 0; columnIndex < tupleDesc->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, columnIndex);

		if (column->attisdropped)
			continue;

		if (!IsGeometryTypeId(column->atttypid))
			continue;

		GeometryColumn *geoColumn = palloc0(sizeof(GeometryColumn));

		geoColumn->name = pstrdup(NameStr(column->attname));
		geoColumn->geometryType = GEOMETRY_GET_TYPE(column->atttypmod);

		geometryColumns = lappend(geometryColumns, geoColumn);
	}

	return geometryColumns;
}


/*
 * Utility to return if a given relation has any geometry column type.
 */
bool
TupleDescHasGeometryColumn(TupleDesc tupleDesc)
{
	bool		hasGeometryColumn = false;

	for (int columnIndex = 0; columnIndex < tupleDesc->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, columnIndex);

		if (column->attisdropped)
			continue;

		if (!IsGeometryTypeId(column->atttypid))
			continue;

		hasGeometryColumn = true;
		break;
	}

	return hasGeometryColumn;
}


/*
 * GetGeometryTypeName gets the name of a geometry type.
 */
static char *
GetGeometryTypeName(int geometryType)
{
	switch (geometryType)
	{
		case GEOMETRY_TYPE_POINT:
			return "point";

		case GEOMETRY_TYPE_LINE:
			return "linestring";

		case GEOMETRY_TYPE_POLYGON:
			return "polygon";

		case GEOMETRY_TYPE_MULTIPOINT:
			return "multipoint";

		case GEOMETRY_TYPE_MULTILINE:
			return "multilinestring";

		case GEOMETRY_TYPE_MULTIPOLYGON:
			return "multipolygon";

		default:
			return NULL;
	}
}
