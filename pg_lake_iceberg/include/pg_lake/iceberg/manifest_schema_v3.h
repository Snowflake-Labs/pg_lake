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

/*
 * Iceberg v3 manifest entry schema.
 *
 * Difference from v2 (see ``manifest_schema_v2.h``): the inner ``data_file``
 * record gains four optional fields that are spec-mandated on v3:
 *
 *   - ``first_row_id``           (field-id 142, long)   -- row lineage start
 *   - ``referenced_data_file``   (field-id 143, string) -- DV pointer
 *   - ``content_offset``         (field-id 144, long)   -- DV byte offset
 *   - ``content_size_in_bytes``  (field-id 145, long)   -- DV byte length
 *
 * All four are marked optional (`["null", T]`) so a writer that does not
 * yet emit them (Stage 10 -- this commit) produces a v3-shaped manifest
 * the moment a v3 dispatch lands; the row-lineage emission turns on at
 * Stage 12 and the DV fields stay null until the DV stages.
 *
 * Spec reference: https://iceberg.apache.org/spec/#manifests
 */

static const char *manifest_schema_v3_json = "\
{ \
  \"type\": \"record\", \
  \"name\": \"manifest_entry\", \
  \"fields\": [ \
    { \
      \"name\": \"status\", \
      \"type\": \"int\", \
      \"field-id\": 0 \
    }, \
    { \
      \"name\": \"snapshot_id\", \
      \"type\": [ \
        \"null\", \
        \"long\" \
      ], \
      \"field-id\": 1 \
    }, \
    { \
      \"name\": \"sequence_number\", \
      \"type\": [ \
        \"null\", \
        \"long\" \
      ], \
      \"doc\": \"Data sequence number of the file\", \
      \"field-id\": 3 \
    }, \
    { \
      \"name\": \"file_sequence_number\", \
      \"type\": [ \
        \"null\", \
        \"long\" \
      ], \
      \"doc\": \"File sequence number indicating when the file was added\", \
      \"field-id\": 4 \
    }, \
    { \
      \"name\": \"data_file\", \
      \"type\": { \
        \"type\": \"record\", \
        \"name\": \"r2\", \
        \"fields\": [ \
          { \
            \"name\": \"content\", \
            \"type\": \"int\", \
            \"doc\": \"Contents of the file: 0=data, 1=position deletes, 2=equality deletes\", \
            \"field-id\": 134 \
          }, \
          { \
            \"name\": \"file_path\", \
            \"type\": \"string\", \
            \"doc\": \"Location URI with FS scheme\", \
            \"field-id\": 100 \
          }, \
          { \
            \"name\": \"file_format\", \
            \"type\": \"string\", \
            \"doc\": \"File format name: avro, orc, or parquet\", \
            \"field-id\": 101 \
          }, \
          { \
            \"name\": \"partition\", \
            \"type\": { \
              \"type\": \"record\", \
              \"name\": \"r102\", \
              \"fields\": [] \
            }, \
            \"doc\": \"Partition data tuple, schema based on the partition spec\", \
            \"field-id\": 102 \
          }, \
          { \
            \"name\": \"record_count\", \
            \"type\": \"long\", \
            \"doc\": \"Number of records in the file\", \
            \"field-id\": 103 \
          }, \
          { \
            \"name\": \"file_size_in_bytes\", \
            \"type\": \"long\", \
            \"doc\": \"Total file size in bytes\", \
            \"field-id\": 104 \
          }, \
          { \
            \"name\": \"column_sizes\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": { \
                  \"type\": \"record\", \
                  \"name\": \"k117_v118\", \
                  \"fields\": [ \
                    { \
                      \"name\": \"key\", \
                      \"type\": \"int\", \
                      \"field-id\": 117 \
                    }, \
                    { \
                      \"name\": \"value\", \
                      \"type\": \"long\", \
                      \"field-id\": 118 \
                    } \
                  ] \
                }, \
                \"logicalType\": \"map\" \
              } \
            ], \
            \"doc\": \"Map of column id to total size on disk\", \
            \"field-id\": 108 \
          }, \
          { \
            \"name\": \"value_counts\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": { \
                  \"type\": \"record\", \
                  \"name\": \"k119_v120\", \
                  \"fields\": [ \
                    { \
                      \"name\": \"key\", \
                      \"type\": \"int\", \
                      \"field-id\": 119 \
                    }, \
                    { \
                      \"name\": \"value\", \
                      \"type\": \"long\", \
                      \"field-id\": 120 \
                    } \
                  ] \
                }, \
                \"logicalType\": \"map\" \
              } \
            ], \
            \"doc\": \"Map of column id to total count, including null and NaN\", \
            \"field-id\": 109 \
          }, \
          { \
            \"name\": \"null_value_counts\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": { \
                  \"type\": \"record\", \
                  \"name\": \"k121_v122\", \
                  \"fields\": [ \
                    { \
                      \"name\": \"key\", \
                      \"type\": \"int\", \
                      \"field-id\": 121 \
                    }, \
                    { \
                      \"name\": \"value\", \
                      \"type\": \"long\", \
                      \"field-id\": 122 \
                    } \
                  ] \
                }, \
                \"logicalType\": \"map\" \
              } \
            ], \
            \"doc\": \"Map of column id to null value count\", \
            \"field-id\": 110 \
          }, \
          { \
            \"name\": \"nan_value_counts\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": { \
                  \"type\": \"record\", \
                  \"name\": \"k138_v139\", \
                  \"fields\": [ \
                    { \
                      \"name\": \"key\", \
                      \"type\": \"int\", \
                      \"field-id\": 138 \
                    }, \
                    { \
                      \"name\": \"value\", \
                      \"type\": \"long\", \
                      \"field-id\": 139 \
                    } \
                  ] \
                }, \
                \"logicalType\": \"map\" \
              } \
            ], \
            \"doc\": \"Map of column id to number of NaN values in the column\", \
            \"field-id\": 137 \
          }, \
          { \
            \"name\": \"lower_bounds\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": { \
                  \"type\": \"record\", \
                  \"name\": \"k126_v127\", \
                  \"fields\": [ \
                    { \
                      \"name\": \"key\", \
                      \"type\": \"int\", \
                      \"field-id\": 126 \
                    }, \
                    { \
                      \"name\": \"value\", \
                      \"type\": \"bytes\", \
                      \"field-id\": 127 \
                    } \
                  ] \
                }, \
                \"logicalType\": \"map\" \
              } \
            ], \
            \"doc\": \"Map of column id to lower bound\", \
            \"field-id\": 125 \
          }, \
          { \
            \"name\": \"upper_bounds\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": { \
                  \"type\": \"record\", \
                  \"name\": \"k129_v130\", \
                  \"fields\": [ \
                    { \
                      \"name\": \"key\", \
                      \"type\": \"int\", \
                      \"field-id\": 129 \
                    }, \
                    { \
                      \"name\": \"value\", \
                      \"type\": \"bytes\", \
                      \"field-id\": 130 \
                    } \
                  ] \
                }, \
                \"logicalType\": \"map\" \
              } \
            ], \
            \"doc\": \"Map of column id to upper bound\", \
            \"field-id\": 128 \
          }, \
          { \
            \"name\": \"key_metadata\", \
            \"type\": [ \
              \"null\", \
              \"bytes\" \
            ], \
            \"doc\": \"Encryption key metadata blob\", \
            \"field-id\": 131 \
          }, \
          { \
            \"name\": \"split_offsets\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": \"long\", \
                \"element-id\": 133 \
              } \
            ], \
            \"doc\": \"Splittable offsets\", \
            \"field-id\": 132 \
          }, \
          { \
            \"name\": \"equality_ids\", \
            \"type\": [ \
              \"null\", \
              { \
                \"type\": \"array\", \
                \"items\": \"int\", \
                \"element-id\": 136 \
              } \
            ], \
            \"doc\": \"Equality comparison field IDs\", \
            \"field-id\": 135 \
          }, \
          { \
            \"name\": \"sort_order_id\", \
            \"type\": [ \
              \"null\", \
              \"int\" \
            ], \
            \"doc\": \"Sort order ID\", \
            \"field-id\": 140 \
          }, \
          { \
            \"name\": \"first_row_id\", \
            \"type\": [ \
              \"null\", \
              \"long\" \
            ], \
            \"doc\": \"First row ID assigned by the writer (row lineage)\", \
            \"field-id\": 142 \
          }, \
          { \
            \"name\": \"referenced_data_file\", \
            \"type\": [ \
              \"null\", \
              \"string\" \
            ], \
            \"doc\": \"Path of the data file the deletion vector applies to\", \
            \"field-id\": 143 \
          }, \
          { \
            \"name\": \"content_offset\", \
            \"type\": [ \
              \"null\", \
              \"long\" \
            ], \
            \"doc\": \"Byte offset of the deletion-vector payload\", \
            \"field-id\": 144 \
          }, \
          { \
            \"name\": \"content_size_in_bytes\", \
            \"type\": [ \
              \"null\", \
              \"long\" \
            ], \
            \"doc\": \"Byte length of the deletion-vector payload\", \
            \"field-id\": 145 \
          } \
        ] \
      }, \
      \"field-id\": 2 \
    } \
  ] \
}";
