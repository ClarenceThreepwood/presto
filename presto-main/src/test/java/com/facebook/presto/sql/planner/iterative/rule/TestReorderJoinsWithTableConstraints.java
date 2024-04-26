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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.HANDLE_COMPLEX_EQUI_JOINS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.relational.Expressions.call;

public class TestReorderJoinsWithTableConstraints
        extends BasePlanTest
{
//    private ImmutableMap<String, String> nationColumns = ImmutableMap.<String, String>builder()
//            .put("regionkey", "regionkey")
//            .put("nationkey", "nationkey")
//            .put("name", "name")
//            .put("comment", "comment")
//            .build();
//
//    private ImmutableMap<String, String> orderColumns = ImmutableMap.<String, String>builder()
//            .put("orderpriority", "orderpriority")
//            .put("orderstatus", "orderstatus")
//            .put("totalprice", "totalprice")
//            .put("orderkey", "orderkey")
//            .put("custkey", "custkey")
//            .put("orderdate", "orderdate")
//            .put("comment", "comment")
//            .put("shippriority", "shippriority")
//            .put("clerk", "clerk")
//            .build();

    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;
//    private TableHandle customerTableHandle;

    private ColumnHandle ordersCustKeyColumn;
    private ColumnHandle ordersOrderKeyColumn;
    private ColumnHandle ordersOrderPriorityColumn;
    private ColumnHandle ordersCommentColumn;
    private ColumnHandle ordersOrderStatusColumn;
    private ColumnHandle ordersOrderDateColumn;
    private ColumnHandle ordersShipPriorityColumn;
    private ColumnHandle ordersClerkColumn;
    private ColumnHandle ordersTotalPriceColumn;
    private ColumnHandle nationRegionKeyColumn;
    private ColumnHandle nationNationKeyColumn;
    private ColumnHandle nationNameColumn;
    private ColumnHandle nationCommentColumn;
//    private ColumnHandle customerCustKeyColumn;
//    private ColumnHandle customerNameColumn;
//    private ColumnHandle customerAddressColumn;
//    private ColumnHandle customerNationKeyColumn;
//    private ColumnHandle customerPhoneColumn;
//    private ColumnHandle customerAcctbalColumn;
//    private ColumnHandle customerMktSegmentColumn;
//    private ColumnHandle customerCommentColumn;

    private VariableReferenceExpression ordersCustKeyVariable;
    private VariableReferenceExpression ordersOrderKeyVariable;
    private VariableReferenceExpression ordersOrderPriorityVariable;
    private VariableReferenceExpression ordersCommentVariable;
    private VariableReferenceExpression ordersOrderStatusVariable;
    private VariableReferenceExpression ordersOrderDateVariable;
    private VariableReferenceExpression ordersShipPriorityVariable;
    private VariableReferenceExpression ordersClerkVariable;
    private VariableReferenceExpression ordersTotalPriceVariable;
    private VariableReferenceExpression nationRegionKeyVariable;
    private VariableReferenceExpression nationNationKeyVariable;
    private VariableReferenceExpression nationNameVariable;
    private VariableReferenceExpression nationCommentVariable;
//    private VariableReferenceExpression customerCustKeyVariable;
//    private VariableReferenceExpression customerNameVariable;
//    private VariableReferenceExpression customerAddressVariable;
//    private VariableReferenceExpression customerNationKeyVariable;
//    private VariableReferenceExpression customerPhoneVariable;
//    private VariableReferenceExpression customerAcctbalVariable;
//    private VariableReferenceExpression customerMktSegmentVariable;
//    private VariableReferenceExpression customerCommentVariable;

    private List<VariableReferenceExpression> nationVariables;
    private List<VariableReferenceExpression> ordersVariables;
//    private List<VariableReferenceExpression> customerVariables;
    private Map<VariableReferenceExpression, ColumnHandle> nationColumnMapping;
    private Map<VariableReferenceExpression, ColumnHandle> ordersColumnMapping;
//    private Map<VariableReferenceExpression, ColumnHandle> customerColumnMapping;

    private RuleTester tester;
    private FunctionResolution functionResolution;
    private LogicalPropertiesProviderImpl logicalPropertiesProvider;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(
                        JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name(),
                        JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name(),
                        HANDLE_COMPLEX_EQUI_JOINS, "true",
                        EXPLOIT_CONSTRAINTS, "true"),
                Optional.of(4));
        this.functionResolution = new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        this.logicalPropertiesProvider = new LogicalPropertiesProviderImpl(functionResolution);

        ConnectorId connectorId = tester.getCurrentConnectorId();

        // Set up orders table
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        ordersTableHandle = new TableHandle(
                connectorId,
                ordersTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(ordersTpchTableHandle, TupleDomain.all())));

        ordersCustKeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ordersOrderKeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        ordersOrderPriorityColumn = new TpchColumnHandle("orderpriority", VARCHAR);
        ordersCommentColumn = new TpchColumnHandle("comment", VARCHAR);
        ordersOrderStatusColumn = new TpchColumnHandle("orderstatus", createVarcharType(1));
        ordersOrderDateColumn = new TpchColumnHandle("orderdate", DATE);
        ordersShipPriorityColumn = new TpchColumnHandle("shippriority", INTEGER);
        ordersClerkColumn = new TpchColumnHandle("clerk", createVarcharType(15));
        ordersTotalPriceColumn = new TpchColumnHandle("totalprice", DOUBLE);

        ordersCustKeyVariable = new VariableReferenceExpression(Optional.empty(), "custkey", BIGINT);
        ordersOrderKeyVariable = new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT);
        ordersOrderPriorityVariable = new VariableReferenceExpression(Optional.empty(), "orderpriority", VARCHAR);
        ordersCommentVariable = new VariableReferenceExpression(Optional.empty(), "comment", DOUBLE);
        ordersOrderStatusVariable = new VariableReferenceExpression(Optional.empty(), "orderstatus", VARCHAR);
        ordersOrderDateVariable = new VariableReferenceExpression(Optional.empty(), "orderdate", DATE);
        ordersShipPriorityVariable = new VariableReferenceExpression(Optional.empty(), "shippriority", INTEGER);
        ordersClerkVariable = new VariableReferenceExpression(Optional.empty(), "clerk", createVarcharType(15));
        ordersTotalPriceVariable = new VariableReferenceExpression(Optional.empty(), "totalprice", DOUBLE);

        ordersVariables = ImmutableList.<VariableReferenceExpression>builder()
                .add(ordersCustKeyVariable)
                .add(ordersOrderKeyVariable)
                .add(ordersOrderPriorityVariable)
                //.add(ordersCommentVariable)
                .add(ordersOrderStatusVariable)
                .add(ordersOrderDateVariable)
                .add(ordersShipPriorityVariable)
                .add(ordersClerkVariable)
                .add(ordersTotalPriceVariable)
                .build();

        ordersColumnMapping = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                .put(ordersCustKeyVariable, ordersCustKeyColumn)
                .put(ordersOrderKeyVariable, ordersOrderKeyColumn)
                .put(ordersOrderPriorityVariable, ordersOrderPriorityColumn)
                .put(ordersCommentVariable, ordersCommentColumn)
                .put(ordersOrderStatusVariable, ordersOrderStatusColumn)
                .put(ordersOrderDateVariable, ordersOrderDateColumn)
                .put(ordersShipPriorityVariable, ordersShipPriorityColumn)
                .put(ordersClerkVariable, ordersClerkColumn)
                .put(ordersTotalPriceVariable, ordersTotalPriceColumn)
                .build();

        // Set up nation table
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        nationTableHandle = new TableHandle(
                connectorId,
                nationTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all())));

        nationRegionKeyColumn = new TpchColumnHandle("regionkey", BIGINT);
        nationNationKeyColumn = new TpchColumnHandle("nationkey", BIGINT);
        nationNameColumn = new TpchColumnHandle("name", createVarcharType(25));
        nationCommentColumn = new TpchColumnHandle("comment", VARCHAR);

        nationRegionKeyVariable = new VariableReferenceExpression(Optional.empty(), "regionkey", BIGINT);
        nationNationKeyVariable = new VariableReferenceExpression(Optional.empty(), "nationkey", BIGINT);
        nationNameVariable = new VariableReferenceExpression(Optional.empty(), "name", createVarcharType(25));
        nationCommentVariable = new VariableReferenceExpression(Optional.empty(), "comment", VARCHAR);

        nationVariables = ImmutableList.of(nationRegionKeyVariable, nationNationKeyVariable, nationNameVariable, nationCommentVariable);

        nationColumnMapping = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                .put(nationRegionKeyVariable, nationRegionKeyColumn)
                .put(nationNationKeyVariable, nationNationKeyColumn)
                .put(nationNameVariable, nationNameColumn)
                .put(nationCommentVariable, nationCommentColumn)
                .build();

        // Set up customer table
//        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
//        customerTableHandle = new TableHandle(
//                connectorId,
//                customerTpchTableHandle,
//                TestingTransactionHandle.create(),
//                Optional.of(new TpchTableLayoutHandle(customerTpchTableHandle, TupleDomain.all())));
//
//        customerCustKeyColumn = new TpchColumnHandle("custkey", BIGINT);
//        customerNameColumn = new TpchColumnHandle("name", createVarcharType(25));
//        customerAddressColumn = new TpchColumnHandle("address", createVarcharType(40));
//        customerNationKeyColumn = new TpchColumnHandle("nationkey", BIGINT);
//        customerPhoneColumn = new TpchColumnHandle("phone", createVarcharType(15));
//        customerAcctbalColumn = new TpchColumnHandle("acctbal", DOUBLE);
//        customerMktSegmentColumn = new TpchColumnHandle("mktsegment", createVarcharType(10));
//        customerCommentColumn = new TpchColumnHandle("comment", VARCHAR);
//
//        customerCustKeyVariable = new VariableReferenceExpression(Optional.empty(), "custkey", BIGINT);
//        customerNameVariable = new VariableReferenceExpression(Optional.empty(), "name", createVarcharType(25));
//        customerAddressVariable = new VariableReferenceExpression(Optional.empty(), "address", createVarcharType(40));
//        customerNationKeyVariable = new VariableReferenceExpression(Optional.empty(), "nationkey", BIGINT);
//        customerPhoneVariable = new VariableReferenceExpression(Optional.empty(), "phone", createVarcharType(15));
//        customerAcctbalVariable = new VariableReferenceExpression(Optional.empty(), "acctbal", DOUBLE);
//        customerMktSegmentVariable = new VariableReferenceExpression(Optional.empty(), "mktsegment", createVarcharType(10));
//        customerCommentVariable = new VariableReferenceExpression(Optional.empty(), "comment", VARCHAR);
//
//        customerVariables = ImmutableList.<VariableReferenceExpression>builder()
//                .add(customerCustKeyVariable)
//                .add(customerNameVariable)
//                .add(customerAddressVariable)
//                .add(customerNationKeyVariable)
//                .add(customerPhoneVariable)
//                .add(customerAcctbalVariable)
//                .add(customerMktSegmentVariable)
//                .add(customerCommentVariable)
//                .build();
//
//        customerColumnMapping = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
//                .put(customerCustKeyVariable, customerCustKeyColumn)
//                .put(customerNameVariable, customerNameColumn)
//                .put(customerAddressVariable, customerAddressColumn)
//                .put(customerNationKeyVariable, customerNationKeyColumn)
//                .put(customerPhoneVariable, customerPhoneColumn)
//                .put(customerAcctbalVariable, customerAcctbalColumn)
//                .put(customerMktSegmentVariable, customerMktSegmentColumn)
//                .put(customerCommentVariable, customerCommentColumn)
//                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testCardinalityEstimateWithSingleJoinKey()
    {
        validateTwoWayJoinWithConstraints(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName())),
                375);

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of(nationNationKeyColumn)), true, true, true)),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName())),
                1500000); // CARD(orders)
        validateTwoWayJoinWithConstraints(
                ImmutableList.of(new UniqueConstraint<>(Optional.of("uniq"), new LinkedHashSet<>(ImmutableList.of(nationNationKeyColumn)), true, true, true)),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName())),
                1500000); // CARD(orders)

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(),
                ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of(ordersCustKeyColumn)), true, true, true)),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName())),
                25); // CARD(nation)
        validateTwoWayJoinWithConstraints(
                ImmutableList.of(),
                ImmutableList.of(new UniqueConstraint<>(Optional.of("uniq"), new LinkedHashSet<>(ImmutableList.of(ordersCustKeyColumn)), true, true, true)),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName())),
                25); // CARD(nation)
    }

    @Test
    public void testCardinalityEstimateWithMultipleJoinKeys()
    {
        validateTwoWayJoinWithConstraints(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName())),
                22.5);

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of(nationNationKeyColumn)), true, true, true)),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName())),
                1350000); //1500000 * 0.9

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(),
                ImmutableList.of(new UniqueConstraint<>(Optional.of("uniq"), new LinkedHashSet<>(ImmutableList.of(ordersCustKeyColumn)), true, true, true)),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName())),
                22.5); // 25 * 0.9

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of(nationNationKeyColumn, nationRegionKeyColumn)), true, true, true)),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName())),
                1500000);

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(),
                ImmutableList.of(new UniqueConstraint<>(Optional.of("uniq"), new LinkedHashSet<>(ImmutableList.of(ordersCustKeyColumn, ordersOrderKeyColumn)), true, true, true)),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName())),
                25);

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of(nationRegionKeyColumn)), true, true, true)),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable), new EquiJoinClause(nationNameVariable, ordersOrderStatusVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName()), equiJoinClause(ordersOrderStatusVariable.getName(), nationNameVariable.getName())),
                1215000); // 1500000 * 0.9 * 0.9

        validateTwoWayJoinWithConstraints(
                ImmutableList.of(new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of(nationNationKeyColumn, nationRegionKeyColumn)), true, true, true)),
                ImmutableList.of(),
                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable), new EquiJoinClause(nationRegionKeyVariable, ordersOrderKeyVariable), new EquiJoinClause(nationNameVariable, ordersOrderStatusVariable)),
                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName()), equiJoinClause(ordersOrderKeyVariable.getName(), nationRegionKeyVariable.getName()), equiJoinClause(ordersOrderStatusVariable.getName(), nationNameVariable.getName())),
                1350000); // 1500000 * 0.9
    }

//    @Test
//    public void testCardinalityEstimateWithMultipleJoins()
//    {
//        validateThreeWayJoinWithConstraints(
//                ImmutableList.of(),
//                ImmutableList.of(),
//                ImmutableList.of(),
//                ImmutableList.of(new EquiJoinClause(nationNationKeyVariable, ordersCustKeyVariable)),
//                ImmutableList.of(new EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable)),
//                ImmutableList.of(equiJoinClause(ordersCustKeyVariable.getName(), nationNationKeyVariable.getName())),
//                500);
//    }

    private void validateTwoWayJoinWithConstraints(List<TableConstraint<ColumnHandle>> leftConstraints,
            List<TableConstraint<ColumnHandle>> rightConstraints,
            List<EquiJoinClause> joinClauses,
            List<ExpectedValueProvider<EquiJoinClause>> joinClausesMatcher,
            double outputRowCount)
    {
        Map<String, String> nationAssignments = nationVariables.stream().collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getName));
        Map<String, String> ordersAssignments = ordersVariables.stream().collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getName));
        List<String> tableScanNationNodeId = new ArrayList<>();
        List<String> tableScanOrdersNodeId = new ArrayList<>();
        assertReorderJoins()
                .on(p -> {
                    TableScanNode tableScanNation = p.tableScan(nationTableHandle,
                            nationVariables,
                            nationColumnMapping,
                            leftConstraints);
                    TableScanNode tableScanOrders = p.tableScan(ordersTableHandle,
                            ordersVariables,
                            ordersColumnMapping,
                            rightConstraints);
                    tableScanNationNodeId.add(tableScanNation.getId().toString());
                    tableScanOrdersNodeId.add(tableScanOrders.getId().toString());

                    nationVariables.forEach(variable -> p.variable(variable));
                    ordersVariables.forEach((variable -> p.variable(variable)));

                    return p.join(INNER,
                            tableScanNation,
                            tableScanOrders,
                            joinClauses,
                            ImmutableList.of(ordersShipPriorityVariable),
                            Optional.empty());
                })
                .matches(join(
                        INNER,
                        joinClausesMatcher,
                        Optional.empty(),
                        Optional.empty(),
                        tableScan("orders", ordersAssignments),
                        tableScan("nation", nationAssignments))
                        .withExactOutputs(ordersShipPriorityVariable.getName())
                        .withOutputRowCount(outputRowCount));
    }

//    private void validateThreeWayJoinWithConstraints(List<TableConstraint<ColumnHandle>> nationConstraints,
//            List<TableConstraint<ColumnHandle>> ordersConstraints,
//            List<TableConstraint<ColumnHandle>> customerConstraints,
//            List<EquiJoinClause> nationOrdersJoinClauses,
//            List<EquiJoinClause> ordersCustomerJoinClauses,
//            List<ExpectedValueProvider<EquiJoinClause>> joinClausesMatcher,
//            double outputRowCount)
//    {
//        Map<String, String> nationAssignments = nationVariables.stream().collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getName));
//        Map<String, String> ordersAssignments = ordersVariables.stream().collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getName));
//        Map<String, String> customerAssignments = customerVariables.stream().collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getName));
//        List<String> tableScanNationNodeId = new ArrayList<>();
//        List<String> tableScanOrdersNodeId = new ArrayList<>();
//        List<String> tableScanCustomerNodeId = new ArrayList<>();
//        assertReorderJoins()
//                .on(p -> {
//                    TableScanNode tableScanNation = p.tableScan(nationTableHandle,
//                            nationVariables,
//                            nationColumnMapping,
//                            nationConstraints);
//                    TableScanNode tableScanOrders = p.tableScan(ordersTableHandle,
//                            ordersVariables,
//                            ordersColumnMapping,
//                            ordersConstraints);
//                    TableScanNode tableScanCustomer = p.tableScan(customerTableHandle,
//                            customerVariables,
//                            customerColumnMapping,
//                            customerConstraints);
//                    tableScanNationNodeId.add(tableScanNation.getId().toString());
//                    tableScanOrdersNodeId.add(tableScanOrders.getId().toString());
//                    tableScanCustomerNodeId.add(tableScanCustomer.getId().toString());
//
//                    nationVariables.forEach(variable -> p.variable(variable));
//                    ordersVariables.forEach((variable -> p.variable(variable)));
//                    customerVariables.forEach(variable -> p.variable(variable));
//
//                    return p.join(INNER,
//                            tableScanNation,
//                            //tableScanOrders,
//                            p.join(INNER,
//                                    tableScanOrders,
//                                    tableScanCustomer,
//                                    ordersCustomerJoinClauses,
//                                    ImmutableList.<VariableReferenceExpression>builder()
//                                            .addAll(ordersVariables)
//                                            .addAll(customerVariables)
//                                            .build(),
//                                    Optional.empty()),
//                            nationOrdersJoinClauses,
//                            ImmutableList.of(ordersShipPriorityVariable),
//                            Optional.empty());
//                })
//                .matches(join(
//                        INNER,
//                        joinClausesMatcher,
//                        Optional.empty(),
//                        Optional.empty(),
//                        tableScan("orders", ordersAssignments),
//                        tableScan("nation", nationAssignments))
//                        .withExactOutputs(ordersShipPriorityVariable.getName())
//                        .withOutputRowCount(outputRowCount));
//    }

    private RuleAssert assertReorderJoins()
    {
        return tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1), tester.getMetadata()), logicalPropertiesProvider);
    }

    private RowExpression comparisonRowExpression(OperatorType type, RowExpression left, RowExpression right)
    {
        return call(type.name(), functionResolution.comparisonFunction(type, left.getType(), right.getType()), BOOLEAN, left, right);
    }
}
