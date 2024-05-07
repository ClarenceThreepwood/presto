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
import static com.teradata.tpcds.Table.CALL_CENTER;
import static com.teradata.tpcds.Table.CATALOG_PAGE;
import static com.teradata.tpcds.Table.CATALOG_RETURNS;
import static com.teradata.tpcds.Table.CATALOG_SALES;
import static com.teradata.tpcds.Table.CUSTOMER;
import static com.teradata.tpcds.Table.CUSTOMER_ADDRESS;
import static com.teradata.tpcds.Table.CUSTOMER_DEMOGRAPHICS;
import static com.teradata.tpcds.Table.DATE_DIM;
import static com.teradata.tpcds.Table.HOUSEHOLD_DEMOGRAPHICS;
import static com.teradata.tpcds.Table.INCOME_BAND;
import static com.teradata.tpcds.Table.INVENTORY;
import static com.teradata.tpcds.Table.ITEM;
import static com.teradata.tpcds.Table.PROMOTION;
import static com.teradata.tpcds.Table.REASON;
import static com.teradata.tpcds.Table.SHIP_MODE;
import static com.teradata.tpcds.Table.STORE;
import static com.teradata.tpcds.Table.STORE_RETURNS;
import static com.teradata.tpcds.Table.STORE_SALES;
import static com.teradata.tpcds.Table.TIME_DIM;
import static com.teradata.tpcds.Table.WAREHOUSE;
import static com.teradata.tpcds.Table.WEB_PAGE;
import static com.teradata.tpcds.Table.WEB_RETURNS;
import static com.teradata.tpcds.Table.WEB_SALES;
import static com.teradata.tpcds.Table.WEB_SITE;
import static com.teradata.tpcds.Table.getBaseTables;
import static com.teradata.tpcds.Table.getTable;
import static com.teradata.tpcds.column.CallCenterColumn.CC_CALL_CENTER_ID;
import static com.teradata.tpcds.column.CallCenterColumn.CC_CALL_CENTER_SK;
import static com.teradata.tpcds.column.CatalogPageColumn.CP_CATALOG_PAGE_ID;
import static com.teradata.tpcds.column.CatalogPageColumn.CP_CATALOG_PAGE_SK;
import static com.teradata.tpcds.column.CatalogReturnsColumn.CR_ITEM_SK;
import static com.teradata.tpcds.column.CatalogReturnsColumn.CR_ORDER_NUMBER;
import static com.teradata.tpcds.column.CatalogSalesColumn.CS_ITEM_SK;
import static com.teradata.tpcds.column.CatalogSalesColumn.CS_ORDER_NUMBER;
import static com.teradata.tpcds.column.CustomerAddressColumn.CA_ADDRESS_ID;
import static com.teradata.tpcds.column.CustomerAddressColumn.CA_ADDRESS_SK;
import static com.teradata.tpcds.column.CustomerColumn.C_CUSTOMER_ID;
import static com.teradata.tpcds.column.CustomerColumn.C_CUSTOMER_SK;
import static com.teradata.tpcds.column.CustomerDemographicsColumn.CD_DEMO_SK;
import static com.teradata.tpcds.column.DateDimColumn.D_DATE_ID;
import static com.teradata.tpcds.column.DateDimColumn.D_DATE_SK;
import static com.teradata.tpcds.column.HouseholdDemographicsColumn.HD_DEMO_SK;
import static com.teradata.tpcds.column.IncomeBandColumn.IB_INCOME_BAND_SK;
import static com.teradata.tpcds.column.InventoryColumn.INV_DATE_SK;
import static com.teradata.tpcds.column.InventoryColumn.INV_ITEM_SK;
import static com.teradata.tpcds.column.InventoryColumn.INV_WAREHOUSE_SK;
import static com.teradata.tpcds.column.ItemColumn.I_ITEM_ID;
import static com.teradata.tpcds.column.ItemColumn.I_ITEM_SK;
import static com.teradata.tpcds.column.PromotionColumn.P_PROMO_ID;
import static com.teradata.tpcds.column.PromotionColumn.P_PROMO_SK;
import static com.teradata.tpcds.column.ReasonColumn.R_REASON_ID;
import static com.teradata.tpcds.column.ReasonColumn.R_REASON_SK;
import static com.teradata.tpcds.column.ShipModeColumn.SM_SHIP_MODE_ID;
import static com.teradata.tpcds.column.ShipModeColumn.SM_SHIP_MODE_SK;
import static com.teradata.tpcds.column.StoreColumn.S_STORE_ID;
import static com.teradata.tpcds.column.StoreColumn.S_STORE_SK;
import static com.teradata.tpcds.column.StoreReturnsColumn.SR_ITEM_SK;
import static com.teradata.tpcds.column.StoreReturnsColumn.SR_TICKET_NUMBER;
import static com.teradata.tpcds.column.StoreSalesColumn.SS_ITEM_SK;
import static com.teradata.tpcds.column.StoreSalesColumn.SS_TICKET_NUMBER;
import static com.teradata.tpcds.column.TimeDimColumn.T_TIME_ID;
import static com.teradata.tpcds.column.TimeDimColumn.T_TIME_SK;
import static com.teradata.tpcds.column.WarehouseColumn.W_WAREHOUSE_ID;
import static com.teradata.tpcds.column.WarehouseColumn.W_WAREHOUSE_SK;
import static com.teradata.tpcds.column.WebPageColumn.WP_WEB_PAGE_ID;
import static com.teradata.tpcds.column.WebPageColumn.WP_WEB_PAGE_SK;
import static com.teradata.tpcds.column.WebReturnsColumn.WR_ITEM_SK;
import static com.teradata.tpcds.column.WebReturnsColumn.WR_ORDER_NUMBER;
import static com.teradata.tpcds.column.WebSalesColumn.WS_ITEM_SK;
import static com.teradata.tpcds.column.WebSalesColumn.WS_ORDER_NUMBER;
import static com.teradata.tpcds.column.WebSiteColumn.WEB_SITE_ID;
import static com.teradata.tpcds.column.WebSiteColumn.WEB_SITE_SK;
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
        for (Table tpcdsTable : getBaseTables()) {
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

        Table table = getTable(tpcdsTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpcdsTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, table, tableConstraintsEnabled);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, Table tpcdsTable, boolean tableConstraintsEnabled)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        Map<String, ColumnHandle> columnNameToHandleAssignments = new HashMap<>();
        List<String> notNullColumns = tableConstraintsEnabled ? getNotNullColumns(tpcdsTable) : ImmutableList.of();
        for (Column column : tpcdsTable.getColumns()) {
            ColumnMetadata columnMetadata = ColumnMetadata.builder()
                    .setName(column.getName())
                    .setType(getPrestoType(column.getType()))
                    .setNullable(!notNullColumns.contains(column.getName()))
                    .build();
            columns.add(columnMetadata);
            columnNameToHandleAssignments.put(columnMetadata.getName(), new TpcdsColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        SchemaTableName tableName = new SchemaTableName(schemaName, tpcdsTable.getName());
        return tableConstraintsEnabled ?
                new ConnectorTableMetadata(tableName, columns.build(), ImmutableMap.of(), Optional.empty(), getTableConstraints(tpcdsTable), columnNameToHandleAssignments) :
                new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        TpcdsTableHandle tpcdsTableHandle = (TpcdsTableHandle) tableHandle;

        Table table = getTable(tpcdsTableHandle.getTableName());
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
            for (Table tpcdsTable : getBaseTables()) {
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
            for (Table tpcdsTable : getBaseTables()) {
                builder.add(new SchemaTableName(schemaName, tpcdsTable.getName()));
            }
        }
        return builder.build();
    }

    private static List<TableConstraint<String>> getTableConstraints(Table tpcdsTable)
    {
        switch (tpcdsTable) {
            case CALL_CENTER:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CALL_CENTER.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(CC_CALL_CENTER_SK.getName())),
                        true,
                        true,
                        true));
            case CATALOG_PAGE:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CATALOG_PAGE.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(CP_CATALOG_PAGE_SK.getName())),
                        true,
                        true,
                        true));
            case CATALOG_RETURNS:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CATALOG_RETURNS.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(CR_ITEM_SK.getName(), CR_ORDER_NUMBER.getName())),
                        true,
                        true,
                        true));
            case CATALOG_SALES:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CATALOG_SALES.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(CS_ITEM_SK.getName(), CS_ORDER_NUMBER.getName())),
                        true,
                        true,
                        true));
            case CUSTOMER:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CUSTOMER.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(C_CUSTOMER_SK.getName())),
                        true,
                        true,
                        true));
            case CUSTOMER_ADDRESS:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CUSTOMER_ADDRESS.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(CA_ADDRESS_SK.getName())),
                        true,
                        true,
                        true));
            case CUSTOMER_DEMOGRAPHICS:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(CUSTOMER_DEMOGRAPHICS.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(CD_DEMO_SK.getName())),
                        true,
                        true,
                        true));
            case DATE_DIM:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(DATE_DIM.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(D_DATE_SK.getName())),
                        true,
                        true,
                        true));
            case HOUSEHOLD_DEMOGRAPHICS:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(HOUSEHOLD_DEMOGRAPHICS.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(HD_DEMO_SK.getName())),
                        true,
                        true,
                        true));
            case INCOME_BAND:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(INCOME_BAND.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(IB_INCOME_BAND_SK.getName())),
                        true,
                        true,
                        true));
            case INVENTORY:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(INVENTORY.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(INV_DATE_SK.getName(), INV_ITEM_SK.getName(), INV_WAREHOUSE_SK.getName())),
                        true,
                        true,
                        true));
            case ITEM:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(ITEM.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(I_ITEM_SK.getName())),
                        true,
                        true,
                        true));
            case PROMOTION:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(PROMOTION.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(P_PROMO_SK.getName())),
                        true,
                        true,
                        true));
            case REASON:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(REASON.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(R_REASON_SK.getName())),
                        true,
                        true,
                        true));
            case SHIP_MODE:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(SHIP_MODE.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(SM_SHIP_MODE_SK.getName())),
                        true,
                        true,
                        true));
            case STORE:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(STORE.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(S_STORE_SK.getName())),
                        true,
                        true,
                        true));
            case STORE_RETURNS:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(STORE_RETURNS.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(SR_ITEM_SK.getName(), SR_TICKET_NUMBER.getName())),
                        true,
                        true,
                        true));
            case STORE_SALES:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(STORE_SALES.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(SS_ITEM_SK.getName(), SS_TICKET_NUMBER.getName())),
                        true,
                        true,
                        true));
            case TIME_DIM:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(TIME_DIM.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(T_TIME_SK.getName())),
                        true,
                        true,
                        true));
            case WAREHOUSE:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(WAREHOUSE.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(W_WAREHOUSE_SK.getName())),
                        true,
                        true,
                        true));
            case WEB_PAGE:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(WEB_PAGE.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(WP_WEB_PAGE_SK.getName())),
                        true,
                        true,
                        true));
            case WEB_RETURNS:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(WEB_RETURNS.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(WR_ORDER_NUMBER.getName(), WR_ITEM_SK.getName())),
                        true,
                        true,
                        true));
            case WEB_SALES:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(WEB_SALES.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(WS_ITEM_SK.getName(), WS_ORDER_NUMBER.getName())),
                        true,
                        true,
                        true));
            case WEB_SITE:
                return ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of(WEB_SITE.getName() + "_pk"),
                        new LinkedHashSet<>(ImmutableList.of(WEB_SITE_SK.getName())),
                        true,
                        true,
                        true));
            default:
                throw new PrestoException(NOT_FOUND, format("Unknown tpcds table : %s", tpcdsTable.getName()));
        }
    }

    private static List<String> getNotNullColumns(Table tpcdsTable)
    {
        switch (tpcdsTable) {
            case CALL_CENTER:
                return ImmutableList.of(CC_CALL_CENTER_SK.getName(), CC_CALL_CENTER_ID.getName());
            case CATALOG_PAGE: //check
                return ImmutableList.of(CP_CATALOG_PAGE_SK.getName(), CP_CATALOG_PAGE_ID.getName());
            case CATALOG_RETURNS:
                return ImmutableList.of(CR_ITEM_SK.getName(), CR_ORDER_NUMBER.getName());
            case CATALOG_SALES:
                return ImmutableList.of(CS_ITEM_SK.getName(), CS_ORDER_NUMBER.getName());
            case CUSTOMER:
                return ImmutableList.of(C_CUSTOMER_SK.getName(), C_CUSTOMER_ID.getName());
            case CUSTOMER_ADDRESS:
                return ImmutableList.of(CA_ADDRESS_SK.getName(), CA_ADDRESS_ID.getName());
            case CUSTOMER_DEMOGRAPHICS:
                return ImmutableList.of(CD_DEMO_SK.getName());
            case DATE_DIM:
                return ImmutableList.of(D_DATE_SK.getName(), D_DATE_ID.getName());
            case HOUSEHOLD_DEMOGRAPHICS:
                return ImmutableList.of(HD_DEMO_SK.getName());
            case INCOME_BAND:
                return ImmutableList.of(IB_INCOME_BAND_SK.getName());
            case INVENTORY:
                return ImmutableList.of(INV_DATE_SK.getName(), INV_ITEM_SK.getName(), INV_WAREHOUSE_SK.getName());
            case ITEM:
                return ImmutableList.of(I_ITEM_SK.getName(), I_ITEM_ID.getName());
            case PROMOTION:
                return ImmutableList.of(P_PROMO_SK.getName(), P_PROMO_ID.getName());
            case REASON:
                return ImmutableList.of(R_REASON_SK.getName(), R_REASON_ID.getName());
            case SHIP_MODE:
                return ImmutableList.of(SM_SHIP_MODE_SK.getName(), SM_SHIP_MODE_ID.getName());
            case STORE:
                return ImmutableList.of(S_STORE_SK.getName(), S_STORE_ID.getName());
            case STORE_RETURNS:
                return ImmutableList.of(SR_ITEM_SK.getName(), SR_TICKET_NUMBER.getName());
            case STORE_SALES:
                return ImmutableList.of(SS_ITEM_SK.getName(), SS_TICKET_NUMBER.getName());
            case TIME_DIM:
                return ImmutableList.of(T_TIME_SK.getName(), T_TIME_ID.getName());
            case WAREHOUSE:
                return ImmutableList.of(W_WAREHOUSE_SK.getName(), W_WAREHOUSE_ID.getName());
            case WEB_PAGE:
                return ImmutableList.of(WP_WEB_PAGE_SK.getName(), WP_WEB_PAGE_ID.getName());
            case WEB_RETURNS:
                return ImmutableList.of(WR_ORDER_NUMBER.getName(), WR_ITEM_SK.getName());
            case WEB_SALES:
                return ImmutableList.of(WS_ITEM_SK.getName(), WS_ORDER_NUMBER.getName());
            case WEB_SITE:
                return ImmutableList.of(WEB_SITE_SK.getName(), WEB_SITE_ID.getName());
            default:
                throw new PrestoException(NOT_FOUND, format("Unknown tpcds table : %s", tpcdsTable.getName()));
        }
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
