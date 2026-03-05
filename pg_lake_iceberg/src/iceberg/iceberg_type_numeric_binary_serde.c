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
#include "utils/builtins.h"
#include "utils/numeric.h"

#include "pg_lake/iceberg/iceberg_type_numeric_binary_serde.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/numeric.h"


static unsigned char *NumericStrToBigEndianBinary(const char *numericStr);
static char *BigEndianBinaryToNumericStr(unsigned char *numericBinary, int scale, bool isNegative);
static char *NormalizeNumericStr(const char *numericStr, int targetScale);
static void TwosComplement(unsigned char *numericBinary);
static unsigned char *StripLeadingBytes(const unsigned char *binaryValue, size_t *binaryLen, bool isNegative);
static unsigned char *SignExtendNumericBinary(const unsigned char *numericBinary, size_t binaryLen);


/*
 * PGNumericIcebergBinarySerialize converts Postgres numeric datum to 128-bit numeric
 * binary in according to Iceberg spec. (2s complement binary in big endian, with minimum
 * number of bytes)
 */
unsigned char *
PGNumericIcebergBinarySerialize(Datum numericDatum, int icebergScale,
								size_t *binaryLen)
{
	Numeric numeric = DatumGetNumeric(numericDatum);

	/*
	 * special values are not allowed by icebeg spec. Instead of hard error,
	 * lets return NULL.
	 */
	if (numeric_is_nan(numeric) || numeric_is_inf(numeric))
	{
		*binaryLen = 0;
		return NULL;
	}

	char	   *numericStr = DatumGetCString(DirectFunctionCall1(numeric_out, numericDatum));

	if (numericStr == NULL || strlen(numericStr) == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("failed to convert numeric to string")));
	}

	bool		isNegative = (*numericStr == '-');

	char	   *normalizedNumericStr = NormalizeNumericStr(numericStr,
														   icebergScale);

	unsigned char *numericBinary = NumericStrToBigEndianBinary(normalizedNumericStr);

	if (isNegative)
	{
		TwosComplement(numericBinary);
	}

	return StripLeadingBytes(numericBinary, binaryLen, isNegative);
}


/*
 * PGNumericIcebergBinaryDeserialize converts a 128-bit numeric binary in big endian
 * to Postgres numeric datum according to Iceberg spec.
 */
Datum
PGNumericIcebergBinaryDeserialize(unsigned char *numericBinary, size_t binaryLen, PGType pgType)
{
	int			precision;
	int			scale;

	GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(pgType.postgresTypeMod,
														 &precision, &scale);

	bool		isNegative = (numericBinary[0] & 0x80) != 0;

	unsigned char *adjustedNumericBinary = SignExtendNumericBinary(numericBinary, binaryLen);

	if (isNegative)
	{
		TwosComplement(adjustedNumericBinary);
	}

	char	   *numericStr = BigEndianBinaryToNumericStr(adjustedNumericBinary, scale, isNegative);

	return DirectFunctionCall3(numeric_in, CStringGetDatum(numericStr),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(pgType.postgresTypeMod));
}


/*
 * NumericStrToBigEndianBinary converts a numeric string (without sign)
 * into a 128-bit big endian binary.
 */
static unsigned char *
NumericStrToBigEndianBinary(const char *numericStr)
{
	unsigned char *numericBinary = palloc0(16);

	for (const char *p = numericStr; *p; p++)
	{
		if (!isdigit(*p))
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("not a valid numeric string while converting to "
								   "Iceberg binary")));

		int			digit = *p - '0';

		/* Multiply by 10 */
		unsigned int carry = 0;

		for (int i = 15; i >= 0; i--)
		{
			unsigned int val = numericBinary[i] * 10U + carry;

			numericBinary[i] = (unsigned char) (val & 0xFF);
			carry = val >> 8;
		}

		if (carry != 0)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("numeric overflow while converting to "
								   "Iceberg binary")));

		/* Add digit */
		carry = (unsigned int) digit;

		for (int i = 15; i >= 0 && carry > 0; i--)
		{
			unsigned int val = numericBinary[i] + carry;

			numericBinary[i] = (unsigned char) (val & 0xFF);
			carry = val >> 8;
		}

		if (carry != 0)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("numeric overflow while converting to "
								   "Iceberg binary")));
	}

	return numericBinary;
}


/*
 * BigEndianBinaryToNumericStr converts a 128-bit big endian binary to a
 * Postgres numeric string.
 */
static char *
BigEndianBinaryToNumericStr(unsigned char *numericBinary, int scale,
							bool isNegative)
{
	/* allocate numericStr to have max digits possible */
	char	   *numericStr = palloc0(40);
	int			totalDigits = 0;
	bool		allZero = false;

	while (!allZero)
	{
		/* Divide by 10 */
		unsigned int remainder = 0;

		for (int i = 0; i < 16; i++)
		{
			unsigned int val = (remainder << 8) | numericBinary[i];

			numericBinary[i] = (unsigned char) (val / 10);
			remainder = val % 10;
		}

		/* Append the remainder as the next numeric digit */
		numericStr[totalDigits++] = '0' + remainder;

		/* Check if the value has become 0 */
		allZero = true;

		for (int i = 0; i < 16; i++)
		{
			if (numericBinary[i] != 0)
			{
				allZero = false;
				break;
			}
		}
	}

	/* Reverse the numeric string */
	for (int i = 0; i < totalDigits / 2; i++)
	{
		char		tmp = numericStr[i];

		numericStr[i] = numericStr[totalDigits - i - 1];
		numericStr[totalDigits - i - 1] = tmp;
	}
	numericStr[totalDigits] = '\0';

	/* Step 3: Add the decimal point based on the scale */
	int			totalLen = (scale >= totalDigits) ? (scale + 2) : (totalDigits + 1);
	char	   *finalizedNumericStr = palloc0((totalLen + 1) + (isNegative ? 1 : 0));

	char	   *finalizedNumericStrPtr = finalizedNumericStr;

	if (isNegative)
	{
		*finalizedNumericStrPtr++ = '-';
	}

	if (scale >= totalDigits)
	{
		*finalizedNumericStrPtr++ = '0';
		*finalizedNumericStrPtr++ = '.';
		for (int i = 0; i < scale - totalDigits; i++)
		{
			*finalizedNumericStrPtr++ = '0';
		}
		strcpy(finalizedNumericStrPtr, numericStr);
	}
	else
	{
		int			integralLen = totalDigits - scale;

		strncpy(finalizedNumericStrPtr, numericStr, integralLen);
		finalizedNumericStrPtr += integralLen;
		*finalizedNumericStrPtr++ = '.';
		strcpy(finalizedNumericStrPtr, numericStr + integralLen);
	}

	/* Step 4: Return the decimal string */
	return finalizedNumericStr;
}


/*
 * NormalizeNumericStr normalizes a Postgres numeric string by removing sign
 * and decimal point, producing the unscaled integer string for the given
 * target Iceberg scale.
 *
 * The fractional part is padded with trailing zeros (or truncated) to
 * exactly targetScale digits.  For example:
 *   "7"         with targetScale=9 → "7000000000"
 *   "7.5"       with targetScale=9 → "7500000000"
 *   "7.123456789012" with targetScale=9 → "7123456789"
 */
static char *
NormalizeNumericStr(const char *numericStr, int targetScale)
{
	const char *p = numericStr;

	if (*p == '-' || *p == '+')
	{
		p++;
	}

	/* Find decimal point if any */
	const char *dot = strchr(p, '.');
	int			integralLen,
				fractionLen;

	if (dot)
	{
		integralLen = (int) (dot - p);
		fractionLen = (int) (strlen(dot + 1));
	}
	else
	{
		integralLen = (int) strlen(p);
		fractionLen = 0;
	}

	/*
	 * Build a pure integer string representing the unscaled value at the
	 * target scale.  E.g. "123.45" with targetScale=9 becomes "123450000000".
	 */
	int			totalLen = integralLen + targetScale;
	char	   *normalizedStr = palloc0(totalLen + 1);

	/* Copy integral part */
	memcpy(normalizedStr, p, integralLen);

	/* Copy fractional digits (up to targetScale) */
	int			copyFraction = (fractionLen < targetScale) ? fractionLen : targetScale;

	if (dot && copyFraction > 0)
		memcpy(normalizedStr + integralLen, dot + 1, copyFraction);

	/* Pad remaining fractional positions with zeros */
	for (int i = copyFraction; i < targetScale; i++)
		normalizedStr[integralLen + i] = '0';

	normalizedStr[totalLen] = '\0';

	return normalizedStr;
}


/*
 * TwosComplement negates a 128-bit two's complement value in place.
 * value is in big-endian format.
 */
static void
TwosComplement(unsigned char *numericBinary)
{
	/* Two's complement negation: value = ~value + 1 */
	unsigned int carry = 1;

	/* Invert all bits */
	for (int i = 0; i < 16; i++)
		numericBinary[i] = ~numericBinary[i];

	/* Add 1 */
	for (int i = 15; i >= 0; i--)
	{
		unsigned int val = numericBinary[i] + carry;

		numericBinary[i] = (unsigned char) (val & 0xFF);
		carry = val >> 8;
		if (carry == 0)
			break;
	}
}


/*
 * StripLeadingBytes strips unnecessary leading bytes.
 * If positive: remove leading 0x00 unless it's the only byte.
 * If negative: remove leading 0xFF unless it's the only byte.
 */
static unsigned char *
StripLeadingBytes(const unsigned char *binaryValue, size_t *binaryLen, bool isNegative)
{
	unsigned char *strippedBinaryValue = NULL;

	int			len = 16;

	if (!isNegative)
	{
		/* Positive number. Remove leading 0x00 */
		int			skippedBytes = 0;

		while (skippedBytes < len - 1 && binaryValue[skippedBytes] == 0x00)
		{
			skippedBytes++;
		}

		if (skippedBytes > 0 && binaryValue[skippedBytes] & 0x80)
		{
			/*
			 * we need to keep the first leading zero, otherwise, the number
			 * is treated like negative
			 */
			int			newlen = len - skippedBytes + 1;

			strippedBinaryValue = palloc0(newlen);

			memcpy(strippedBinaryValue + 1, binaryValue + skippedBytes, len - skippedBytes);
			strippedBinaryValue[0] = 0x00;
			*binaryLen = newlen;
		}
		else if (skippedBytes > 0)
		{
			int			newlen = len - skippedBytes;

			strippedBinaryValue = palloc0(newlen);

			memcpy(strippedBinaryValue, binaryValue + skippedBytes, newlen);
			*binaryLen = newlen;
		}
	}
	else
	{
		/* Negative number. Remove leading 0xFF if it's sign extension */
		int			skippedBytes = 0;

		while (skippedBytes < len - 1 && binaryValue[skippedBytes] == 0xFF)
		{
			skippedBytes++;
		}

		if (skippedBytes > 0 && !(binaryValue[skippedBytes] & 0x80))
		{
			/*
			 * we need to keep the first leading 0xFF, otherwise, the number
			 * is treated like positive
			 */
			int			newlen = len - skippedBytes + 1;

			strippedBinaryValue = palloc0(newlen);

			memcpy(strippedBinaryValue + 1, binaryValue + skippedBytes, len - skippedBytes);
			strippedBinaryValue[0] = 0xFF;
			*binaryLen = newlen;
		}
		else if (skippedBytes > 0)
		{
			int			newlen = len - skippedBytes;

			strippedBinaryValue = palloc0(newlen);

			memcpy(strippedBinaryValue, binaryValue + skippedBytes, newlen);
			*binaryLen = newlen;
		}
	}

	if (strippedBinaryValue == NULL)
	{
		*binaryLen = len;
		strippedBinaryValue = palloc0(len);
		memcpy(strippedBinaryValue, binaryValue, len);
	}

	return strippedBinaryValue;
}


/*
 * SignExtendNumericBinary copies n bytes from the input to the least significant
 * bytes of the output, and sign-extends the upper bytes if the number is
 * negative.
 */
static unsigned char *
SignExtendNumericBinary(const unsigned char *numericBinary, size_t binaryLen)
{
	unsigned char *signExtendedNumericBinary = palloc0(16);

	if (binaryLen == 16)
	{
		/* No need to sign extend */
		memcpy(signExtendedNumericBinary, numericBinary, 16);
		return signExtendedNumericBinary;
	}

	/* Clear the output buffer */
	memset(signExtendedNumericBinary, 0, 16);

	/*
	 * Copy the n bytes from the input to the least significant bytes of the
	 * output
	 */
	memcpy(signExtendedNumericBinary + 16 - binaryLen, numericBinary, binaryLen);

	/* Check the sign bit (most significant bit of the first input byte) */
	if (numericBinary[0] & 0x80)
	{
		/* If the number is negative, set the upper 16 - n bytes to 0xFF */
		memset(signExtendedNumericBinary, 0xFF, 16 - binaryLen);
	}

	return signExtendedNumericBinary;
}
