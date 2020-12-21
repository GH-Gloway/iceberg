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

package org.apache.iceberg.spark.procedures;

import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.RewriteDataFilesAction;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that rewrites manifests in a table.
 * <p>
 * <em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected table.
 *
 * @see Actions#rewriteManifests()
 */
class RewriteDataFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("filter_column_name", DataTypes.StringType),
      ProcedureParameter.required("filter_equal", DataTypes.StringType),
      ProcedureParameter.optional("file_size", DataTypes.IntegerType)
  };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("rewritten_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("added_data_files_count", DataTypes.IntegerType, false, Metadata.empty())
  });

  public static ProcedureBuilder builder() {
    return new Builder<RewriteDataFilesProcedure>() {
      @Override
      protected RewriteDataFilesProcedure doBuild() {
        return new RewriteDataFilesProcedure(tableCatalog());
      }
    };
  }

  private RewriteDataFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    String columnName = args.getString(1);
    String equalCondition = args.getString(2);
    long targetSize = args.isNullAt(3) ? 500 * 1024 * 1024 : args.getLong(3);

    return modifyIcebergTable(tableIdent, table -> {
      Actions actions = Actions.forTable(table);

      RewriteDataFilesAction action = actions.rewriteDataFiles();
      RewriteDataFilesActionResult result = action.filter(Expressions.equal(columnName, equalCondition))
              .targetSizeInBytes(targetSize).execute();

      int numRewrittenDataFile = result.deletedDataFiles().size();
      int numAddedDataFiles = result.addedDataFiles().size();
      InternalRow outputRow = newInternalRow(numRewrittenDataFile, numAddedDataFiles);
      return new InternalRow[]{outputRow};
    });
  }

  @Override
  public String description() {
    return "RewriteDataFilesProcedure";
  }
}
