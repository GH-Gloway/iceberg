/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Converter between Flink types and Iceberg type.
 * The conversion is not a 1:1 mapping that not allows back-and-forth conversion. So some information might get lost
 * during the back-and-forth conversion.
 * <p>
 * This inconsistent types:
 * <ul>
 *   <li>map Iceberg UUID type to Flink BinaryType(16)</li>
 *   <li>map Flink VarCharType(_) and CharType(_) to Iceberg String type</li>
 *   <li>map Flink VarBinaryType(_) to Iceberg Binary type</li>
 *   <li>map Flink TimeType(_) to Iceberg Time type (microseconds)</li>
 *   <li>map Flink TimestampType(_) to Iceberg Timestamp without zone type (microseconds)</li>
 *   <li>map Flink LocalZonedTimestampType(_) to Iceberg Timestamp with zone type (microseconds)</li>
 *   <li>map Flink MultiSetType to Iceberg Map type(element, int)</li>
 * </ul>
 * <p>
 */
public class FlinkSchemaUtil {

  public static final String SCHEMA = "schema";

  public static final String DATA_TYPE = "data-type";

  public static final String EXPR = "expr";

  public static final String WATERMARK = "watermark";

  public static final String WATERMARK_ROWTIME = "rowtime";

  public static final String WATERMARK_STRATEGY = "strategy";

  public static final String WATERMARK_STRATEGY_EXPR = WATERMARK_STRATEGY + '.' + EXPR;

  public static final String WATERMARK_STRATEGY_DATA_TYPE = WATERMARK_STRATEGY + '.' + DATA_TYPE;

  private FlinkSchemaUtil() {
  }

  /**
   * Convert the flink table schema to apache iceberg schema.
   * @return
   */
  public static Map<String, String> convertWaterMark(TableSchema schema) {
    DescriptorProperties tableSchemaProps = new DescriptorProperties(true);

    if (!schema.getWatermarkSpecs().isEmpty()) {
      final List<List<String>> watermarkValues = new ArrayList<>();
      for (WatermarkSpec spec : schema.getWatermarkSpecs()) {
        watermarkValues.add(Arrays.asList(
                spec.getRowtimeAttribute(),
                spec.getWatermarkExpr(),
                spec.getWatermarkExprOutputType().getLogicalType().asSerializableString()));
      }
      tableSchemaProps.putIndexedFixedProperties(
              SCHEMA + '.' + WATERMARK,
              Arrays.asList(WATERMARK_ROWTIME, WATERMARK_STRATEGY_EXPR, WATERMARK_STRATEGY_DATA_TYPE),
              watermarkValues);
    }

    return tableSchemaProps.asMap();
  }

  public static List<WatermarkSpec> convertWaterMarkByProperties(Map<String, String> properties) {
    String watermarkPrefixKey = org.apache.flink.table.descriptors.Schema.SCHEMA + '.' + WATERMARK;
    final int watermarkCount = properties.keySet().stream()
            .filter(k -> k.startsWith(watermarkPrefixKey) && k.endsWith('.' + WATERMARK_ROWTIME))
            .mapToInt(k -> 1)
            .sum();
    List<WatermarkSpec> watermarkSpecs = new ArrayList<>();
    if (watermarkCount > 0) {
      for (int i = 0; i < watermarkCount; i++) {
        final String rowtimeKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_ROWTIME;
        final String exprKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_STRATEGY_EXPR;
        final String typeKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_STRATEGY_DATA_TYPE;

        final String rowtime = properties.get(rowtimeKey);
        final String exprString = properties.get(exprKey);
        final String typeString = properties.get(typeKey);

        if (rowtime == null || exprString == null || typeString == null) {
          throw new ValidationException("Could not find required property rowtimeKey/exprKey/typeKey.");
        }
        final DataType exprType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(typeString));
        watermarkSpecs.add(new WatermarkSpec(rowtime, exprString, exprType));
      }
    }
    return watermarkSpecs;
  }

  /**
   * Convert the flink table schema to apache iceberg schema.
   */
  public static Schema convert(TableSchema schema) {
    LogicalType schemaType = schema.toRowDataType().getLogicalType();
    Preconditions.checkArgument(schemaType instanceof RowType, "Schema logical type should be RowType.");

    RowType root = (RowType) schemaType;
    Type converted = root.accept(new FlinkTypeToType(root));

    return new Schema(converted.asStructType().fields());
  }

  /**
   * Convert a Flink {@link TableSchema} to a {@link Schema} based on the given schema.
   * <p>
   * This conversion does not assign new ids; it uses ids from the base schema.
   * <p>
   * Data types, field order, and nullability will match the Flink type. This conversion may return
   * a schema that is not compatible with base schema.
   *
   * @param baseSchema a Schema on which conversion is based
   * @param flinkSchema a Flink TableSchema
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted or there are missing ids
   */
  public static Schema convert(Schema baseSchema, TableSchema flinkSchema) {
    // convert to a type with fresh ids
    Types.StructType struct = convert(flinkSchema).asStruct();
    // reassign ids to match the base schema
    Schema schema = TypeUtil.reassignIds(new Schema(struct.fields()), baseSchema);
    // fix types that can't be represented in Flink (UUID)
    return FlinkFixupTypes.fixup(schema, baseSchema);
  }

  /**
   * Convert a {@link Schema} to a {@link RowType Flink type}.
   *
   * @param schema a Schema
   * @return the equivalent Flink type
   * @throws IllegalArgumentException if the type cannot be converted to Flink
   */
  public static RowType convert(Schema schema) {
    return (RowType) TypeUtil.visit(schema, new TypeToFlinkType());
  }

  /**
   * Convert a {@link Type} to a {@link LogicalType Flink type}.
   *
   * @param type a Type
   * @return the equivalent Flink type
   * @throws IllegalArgumentException if the type cannot be converted to Flink
   */
  public static LogicalType convert(Type type) {
    return TypeUtil.visit(type, new TypeToFlinkType());
  }

  /**
   * Convert a {@link RowType} to a {@link TableSchema}.
   *
   * @param rowType a RowType
   * @return Flink TableSchema
   */
  public static TableSchema toSchema(RowType rowType) {
    TableSchema.Builder builder = TableSchema.builder();
    for (RowType.RowField field : rowType.getFields()) {
      builder.field(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }
    return builder.build();
  }

  public static TableSchema toSchema(RowType rowType, Map<String, String> properties) {
    TableSchema.Builder builder = TableSchema.builder();
    for (RowType.RowField field : rowType.getFields()) {
      builder.field(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }

    // load watermark
    List<WatermarkSpec> watermarkSpecs = convertWaterMarkByProperties(properties);
    for (WatermarkSpec watermarkSpec : watermarkSpecs) {
      builder.watermark(watermarkSpec);
    }

    return builder.build();
  }
}
