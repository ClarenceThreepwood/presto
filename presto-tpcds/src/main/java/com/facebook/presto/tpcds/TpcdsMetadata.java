/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tpcds;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.tpcds.statistics.TpcdsTableStatisticsFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TpcdsMetadata
        implements ConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            TINY_SCHEMA_NAME, "sf1", "sf10", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    private final Set<String> tableNames;
    private final TpcdsTableStatisticsFactory tpcdsTableStatisticsFactory = new TpcdsTableStatisticsFactory();
    private final boolean tableConstraintsEnabled;

    public TpcdsMetadata(boolean tableConstraintsEnabled)
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (Table tpcdsTable : Table.getBaseTables()) {
            tableNames.add(tpcdsTable.getName().toLowerCase(ENGLISH));
        }
        this.tableNames = tableNames.build();
        this.tableConstraintsEnabled = tableConstraintsEnabled;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }

        // parse the scale factor
        double scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor <= 0) {
            return null;
        }

        return new TpcdsTableHandle(tableName.getTableName(), scaleFactor);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        TpcdsTableHandle tableHandle = (TpcdsTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new TpcdsTableLayoutHandle(tableHandle),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TpcdsTableLayoutHandle layout = (TpcdsTableLayoutHandle) handle;

        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpcdsTableHandle tpcdsTableHandle = (TpcdsTableHandle) tableHandle;

        Table table = Table.getTable(tpcdsTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpcdsTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, table, tableConstraintsEnabled);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, Table tpcdsTable, boolean tableConstraintsEnabled)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        Map<String, ColumnHandle> columnNameToHandleAssignments = new HashMap<>();
        for (Column column : tpcdsTable.getColumns()) {
            ColumnMetadata columnMetadata = new ColumnMetadata(column.getName(), getPrestoType(column.getType()));
            columns.add(columnMetadata);
            columnNameToHandleAssignments.put(columnMetadata.getName(), new TpcdsColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        SchemaTableName tableName = new SchemaTableName(schemaName, tpcdsTable.getName());
        if (tableConstraintsEnabled) {
            return new ConnectorTableMetadata(tableName, columns.build(), ImmutableMap.of(), Optional.empty(), getTableConstraints(tpcdsTable), columnNameToHandleAssignments);
        }
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    private static List<TableConstraint<String>> getTableConstraints(Table tpcdsTable)
    {
        switch (tpcdsTable.getName()) {
            case "call_center":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("call_center_pk"), new LinkedHashSet<>(ImmutableList.of("cc_call_center_sk")), true, true, true));
            case "catalog_page":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("catalog_page_pk"), new LinkedHashSet<>(ImmutableList.of("cp_catalog_page_sk")), true, true, true));
            case "catalog_returns":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("catalog_returns_pk"), new LinkedHashSet<>(ImmutableList.of("cr_item_sk", "cr_order_number")), true, true, true));
            case "catalog_sales":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("catalog_sales_pk"), new LinkedHashSet<>(ImmutableList.of("cs_item_sk", "cs_order_number")), true, true, true));
            case "customer":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("customer_pk"), new LinkedHashSet<>(ImmutableList.of("c_customer_sk")), true, true, true));
            case "customer_address":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("customer_address_pk"), new LinkedHashSet<>(ImmutableList.of("ca_address_sk")), true, true, true));
            case "customer_demographics":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("customer_demographics_pk"), new LinkedHashSet<>(ImmutableList.of("cd_demo_sk")), true, true, true));
            case "date_dim":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("date_dim_pk"), new LinkedHashSet<>(ImmutableList.of("d_date_sk")), true, true, true));
            case "household_demographics":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("household_demographics_pk"), new LinkedHashSet<>(ImmutableList.of("hd_demo_sk")), true, true, true));
            case "income_band":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("income_band_pk"), new LinkedHashSet<>(ImmutableList.of("ib_income_band_sk")), true, true, true));
            case "inventory":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("inventory_pk"), new LinkedHashSet<>(ImmutableList.of("inv_date_sk", "inv_item_sk", "inv_warehouse_sk")), true, true, true));
            case "item":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("item_pk"), new LinkedHashSet<>(ImmutableList.of("i_item_sk")), true, true, true));
            case "promotion":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("promotion_pk"), new LinkedHashSet<>(ImmutableList.of("p_promo_sk")), true, true, true));
            case "reason":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("reason_pk"), new LinkedHashSet<>(ImmutableList.of("r_reason_sk")), true, true, true));
            case "ship_mode":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("ship_mode_pk"), new LinkedHashSet<>(ImmutableList.of("sm_ship_mode_sk")), true, true, true));
            case "store":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("store_pk"), new LinkedHashSet<>(ImmutableList.of("s_store_sk")), true, true, true));
            case "store_returns":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("store_returns_pk"), new LinkedHashSet<>(ImmutableList.of("sr_item_sk", "sr_ticket_number")), true, true, true));
            case "store_sales":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("store_sales_pk"), new LinkedHashSet<>(ImmutableList.of("ss_item_sk", "ss_ticket_number")), true, true, true));
            case "time_dim":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("time_dim_pk"), new LinkedHashSet<>(ImmutableList.of("t_time_sk")), true, true, true));
            case "warehouse":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("warehouse_pk"), new LinkedHashSet<>(ImmutableList.of("w_warehouse_sk")), true, true, true));
            case "web_page":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("web_page_pk"), new LinkedHashSet<>(ImmutableList.of("wp_web_page_sk")), true, true, true));
            case "web_returns":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("web_returns_pk"), new LinkedHashSet<>(ImmutableList.of("wr_order_number", "wr_item_sk")), true, true, true));
            case "web_sales":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("web_sales_pk"), new LinkedHashSet<>(ImmutableList.of("ws_item_sk", "ws_order_number")), true, true, true));
            case "web_site":
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("web_site_pk"), new LinkedHashSet<>(ImmutableList.of("web_site_sk")), true, true, true));
            default:
                throw new PrestoException(NOT_FOUND, format("Unknown tpcds table : %s", tpcdsTable.getName()));
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        TpcdsTableHandle tpcdsTableHandle = (TpcdsTableHandle) tableHandle;

        Table table = Table.getTable(tpcdsTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpcdsTableHandle.getScaleFactor());

        return tpcdsTableStatisticsFactory.create(schemaName, table, columnHandles);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpcdsColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        String columnName = ((TpcdsColumnHandle) columnHandle).getColumnName();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(format("Table %s does not have column %s", tableMetadata.getTable(), columnName));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(session, Optional.ofNullable(prefix.getSchemaName()))) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                if (prefix.getTableName() == null || tpcdsTable.getName().equals(prefix.getTableName())) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpcdsTable, tableConstraintsEnabled);
                    tableColumns.put(new SchemaTableName(schemaName, tpcdsTable.getName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(session, filterSchema)) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                builder.add(new SchemaTableName(schemaName, tpcdsTable.getName()));
            }
        }
        return builder.build();
    }

    private List<String> getSchemaNames(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            return listSchemaNames(session);
        }
        if (schemaNameToScaleFactor(schemaName.get()) > 0) {
            return ImmutableList.of(schemaName.get());
        }
        return ImmutableList.of();
    }

    private static String scaleFactorSchemaName(double scaleFactor)
    {
        return "sf" + scaleFactor;
    }

    public static double schemaNameToScaleFactor(String schemaName)
    {
        if (TINY_SCHEMA_NAME.equals(schemaName)) {
            return TINY_SCALE_FACTOR;
        }

        if (!schemaName.startsWith("sf")) {
            return -1;
        }

        try {
            return Double.parseDouble(schemaName.substring(2));
        }
        catch (Exception ignored) {
            return -1;
        }
    }

    public static Type getPrestoType(ColumnType tpcdsType)
    {
        switch (tpcdsType.getBase()) {
            case IDENTIFIER:
                return BigintType.BIGINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case DATE:
                return DateType.DATE;
            case DECIMAL:
                return createDecimalType(tpcdsType.getPrecision().get(), tpcdsType.getScale().get());
            case CHAR:
                return createCharType(tpcdsType.getPrecision().get());
            case VARCHAR:
                return createVarcharType(tpcdsType.getPrecision().get());
            case TIME:
                return TimeType.TIME;
        }
        throw new IllegalArgumentException("Unsupported TPC-DS type " + tpcdsType);
    }
}
