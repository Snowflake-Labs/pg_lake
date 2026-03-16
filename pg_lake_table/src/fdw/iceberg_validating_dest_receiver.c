/*
 * Copyright 2026 Snowflake Inc.
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
#include "executor/tuptable.h"
#include "pg_lake/fdw/iceberg_validating_dest_receiver.h"
#include "pg_lake/pgduck/iceberg_datum_validation.h"
#include "tcop/dest.h"
#include "utils/lsyscache.h"


typedef struct IcebergValidatingDestReceiverData
{
	DestReceiver pub;
	DestReceiver *child;
	IcebergOutOfRangePolicy outOfRangePolicy;
	TupleDesc	tupleDesc;
}			IcebergValidatingDestReceiverData;


static void
IcebergValidatingStartup(DestReceiver *dest, int operation, TupleDesc tupleDesc)
{
	IcebergValidatingDestReceiverData *self =
		(IcebergValidatingDestReceiverData *) dest;

	self->tupleDesc = tupleDesc;
	self->child->rStartup(self->child, operation, tupleDesc);
}


/*
 * Validate and clamp slot values in-place, then forward to the child.
 */
static bool
IcebergValidatingReceiveSlot(TupleTableSlot *slot, DestReceiver *dest)
{
	IcebergValidatingDestReceiverData *self =
		(IcebergValidatingDestReceiverData *) dest;

	slot_getallattrs(slot);

	TupleDesc	tupleDesc = self->tupleDesc;
	int			natts = tupleDesc->natts;

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped || slot->tts_isnull[i])
			continue;

		if (!IsTemporalType(attr->atttypid) && attr->atttypid != NUMERICOID)
			continue;

		bool		isNull = false;
		Datum		clamped = IcebergErrorOrClampDatum(slot->tts_values[i],
													   attr->atttypid,
													   self->outOfRangePolicy,
													   &isNull);

		slot->tts_values[i] = clamped;
		slot->tts_isnull[i] = isNull;
	}

	return self->child->receiveSlot(slot, self->child);
}


static void
IcebergValidatingShutdown(DestReceiver *dest)
{
	IcebergValidatingDestReceiverData *self =
		(IcebergValidatingDestReceiverData *) dest;

	self->child->rShutdown(self->child);
}


static void
IcebergValidatingDestroy(DestReceiver *dest)
{
	IcebergValidatingDestReceiverData *self =
		(IcebergValidatingDestReceiverData *) dest;

	self->child->rDestroy(self->child);
	pfree(self);
}


/*
 * CreateIcebergValidatingDestReceiver wraps the given child DestReceiver
 * so that every tuple is validated against Iceberg write constraints
 * (temporal boundaries, numeric NaN) before the child sees it.
 *
 * The slot is modified in-place; downstream consumers (partition
 * transforms, CSV serialization) receive already-validated data.
 */
DestReceiver *
CreateIcebergValidatingDestReceiver(DestReceiver *child,
									IcebergOutOfRangePolicy policy)
{
	IcebergValidatingDestReceiverData *self =
		palloc0(sizeof(IcebergValidatingDestReceiverData));

	self->pub.rStartup = IcebergValidatingStartup;
	self->pub.receiveSlot = IcebergValidatingReceiveSlot;
	self->pub.rShutdown = IcebergValidatingShutdown;
	self->pub.rDestroy = IcebergValidatingDestroy;
	self->pub.mydest = DestCopyOut;

	self->child = child;
	self->outOfRangePolicy = policy;

	return (DestReceiver *) self;
}


DestReceiver *
GetIcebergValidatingDestReceiverChild(DestReceiver *dest)
{
	IcebergValidatingDestReceiverData *self =
		(IcebergValidatingDestReceiverData *) dest;

	return self->child;
}
