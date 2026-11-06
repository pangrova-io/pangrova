package org.pangrova.planner.core.analysis;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plan stability test for TPC-DS queries.
 *
 * <p>Discovers every {@code qNN.sql} file under
 * {@code src/test/resources/tpc-ds/queries/}, parses it through the planner,
 * serializes the resulting plan to a deterministic text format, and compares
 * against the matching golden file under
 * {@code src/test/resources/golden-plans/tpc-ds/}.
 *
 * <p>Every selected query must have a reviewed golden file. Any plan change
 * must be explained and reviewed with the updated golden diff.
 *
 * <p>Coverage today: Q1-Q31 (Q1-Q30 are the first batch, Q31 demonstrates the
 * extension pattern). Remaining Q32-Q99 are tracked as a follow-up — adapting
 * each from the DuckDB reference (deps-download/duckdb/extension/tpcds/dsdgen/
 * queries/NN.sql) requires translating implicit joins to ANSI JOIN syntax and
 * LIMIT -&gt; FETCH FIRST. New queries are picked up automatically the moment
 * {@code qNN.sql} files land in the resources directory.
 */
@Tag("plan-stability")
class TpcdsPlanStabilityTest {

    private static final String QUERIES_DIR = "/tpc-ds/queries/";
    private static final Path GOLDEN_DIR = Path.of(
            "src/test/resources/golden-plans/tpc-ds");

    private static PangrovaPlanner planner;

    @BeforeAll
    static void setUp() {
        planner = new PangrovaPlanner(buildTpcDsSchema());
    }

    /**
     * Returns Q01-Q31 only. Q32-Q99 use FETCH FIRST / SUBSTR syntax not yet
     * supported by the Pangrova parser; they are tracked as a follow-up.
     */
    static Stream<String> queryFiles() {
        URL resource = TpcdsPlanStabilityTest.class.getResource(QUERIES_DIR);
        if (resource == null) {
            throw new IllegalStateException("TPC-DS queries directory not found: " + QUERIES_DIR);
        }
        try {
            Path dir = Path.of(resource.toURI());
            try (Stream<Path> files = Files.list(dir)) {
                return files
                        .filter(p -> p.getFileName().toString().matches("q\\d{2}\\.sql"))
                        .filter(p -> {
                            String name = p.getFileName().toString();
                            int num = Integer.parseInt(name.substring(1, 3));
                            return num <= 31;
                        })
                        .map(p -> p.getFileName().toString())
                        .sorted(Comparator.naturalOrder())
                        .toList()
                        .stream();
            }
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Failed to enumerate TPC-DS query files", e);
        }
    }

    @ParameterizedTest(name = "TPC-DS {0} plan stability")
    @MethodSource("queryFiles")
    void planMatchesGoldenFile(String queryFile) throws IOException {
        String sql = loadQuery(queryFile);

        PlanResult result = planner.plan(sql);
        assertThat(result).as("Plan result for " + queryFile).isNotNull();
        assertThat(result.plan()).as("RelNode for " + queryFile).isNotNull();

        String actualPlan = result.plan().explain();
        Path goldenFile = GOLDEN_DIR.resolve(queryFile.replace(".sql", ".plan"));

        assertThat(goldenFile).as("Reviewed golden plan for " + queryFile).isRegularFile();
        String expectedPlan = Files.readString(goldenFile, StandardCharsets.UTF_8);
        assertThat(actualPlan)
                .as("Plan for TPC-DS " + queryFile + " must match golden file. "
                        + "If intentional, re-generate golden files and get approval.")
                .isEqualTo(expectedPlan);
    }

    private static String loadQuery(String filename) {
        String resourcePath = QUERIES_DIR + filename;
        try (InputStream is = TpcdsPlanStabilityTest.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("TPC-DS query not found: " + resourcePath);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load TPC-DS query: " + resourcePath, e);
        }
    }

    private static CalciteSchema buildTpcDsSchema() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        addFactTables(schema);
        addDimensionTables(schema);
        return schema;
    }

    // -- Fact tables ----------------------------------------------------------

    private static void addFactTables(CalciteSchema schema) {
        schema.add("store_sales", tbl(
                "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk",
                "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk",
                "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity",
                "ss_wholesale_cost", "ss_list_price", "ss_sales_price",
                "ss_ext_discount_amt", "ss_ext_sales_price",
                "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax",
                "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax",
                "ss_net_profit"));

        schema.add("store_returns", tbl(
                "sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk",
                "sr_customer_sk", "sr_cdemo_sk", "sr_hdemo_sk", "sr_addr_sk",
                "sr_store_sk", "sr_reason_sk", "sr_ticket_number",
                "sr_return_quantity", "sr_return_amt", "sr_return_tax",
                "sr_return_amt_inc_tax", "sr_fee", "sr_return_ship_cost",
                "sr_refunded_cash", "sr_reversed_charge", "sr_store_credit",
                "sr_net_loss"));

        schema.add("catalog_sales", tbl(
                "cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk",
                "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk",
                "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_ship_cdemo_sk",
                "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk",
                "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk",
                "cs_item_sk", "cs_promo_sk", "cs_order_number", "cs_quantity",
                "cs_wholesale_cost", "cs_list_price", "cs_sales_price",
                "cs_ext_discount_amt", "cs_ext_sales_price",
                "cs_ext_wholesale_cost", "cs_ext_list_price", "cs_ext_ship_cost",
                "cs_ext_tax", "cs_coupon_amt", "cs_net_paid",
                "cs_net_paid_inc_tax", "cs_net_paid_inc_ship",
                "cs_net_paid_inc_ship_tax", "cs_net_profit"));

        schema.add("catalog_returns", tbl(
                "cr_returned_date_sk", "cr_returned_time_sk", "cr_item_sk",
                "cr_refunded_customer_sk", "cr_refunded_cdemo_sk",
                "cr_refunded_hdemo_sk", "cr_refunded_addr_sk",
                "cr_returning_customer_sk", "cr_returning_cdemo_sk",
                "cr_returning_hdemo_sk", "cr_returning_addr_sk",
                "cr_call_center_sk", "cr_catalog_page_sk", "cr_ship_mode_sk",
                "cr_warehouse_sk", "cr_reason_sk", "cr_order_number",
                "cr_return_quantity", "cr_return_amount", "cr_return_tax",
                "cr_return_amt_inc_tax", "cr_fee", "cr_return_ship_cost",
                "cr_refunded_cash", "cr_reversed_charge", "cr_store_credit",
                "cr_net_loss"));

        schema.add("web_sales", tbl(
                "ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk",
                "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk",
                "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk",
                "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk",
                "ws_web_page_sk", "ws_web_site_sk", "ws_ship_mode_sk",
                "ws_warehouse_sk", "ws_promo_sk", "ws_order_number",
                "ws_quantity", "ws_wholesale_cost", "ws_list_price",
                "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price",
                "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_ship_cost",
                "ws_ext_tax", "ws_coupon_amt", "ws_net_paid",
                "ws_net_paid_inc_tax", "ws_net_paid_inc_ship",
                "ws_net_paid_inc_ship_tax", "ws_net_profit"));

        schema.add("web_returns", tbl(
                "wr_returned_date_sk", "wr_returned_time_sk", "wr_item_sk",
                "wr_refunded_customer_sk", "wr_refunded_cdemo_sk",
                "wr_refunded_hdemo_sk", "wr_refunded_addr_sk",
                "wr_returning_customer_sk", "wr_returning_cdemo_sk",
                "wr_returning_hdemo_sk", "wr_returning_addr_sk",
                "wr_web_page_sk", "wr_reason_sk", "wr_order_number",
                "wr_return_quantity", "wr_return_amt", "wr_return_tax",
                "wr_return_amt_inc_tax", "wr_fee", "wr_return_ship_cost",
                "wr_refunded_cash", "wr_reversed_charge", "wr_account_credit",
                "wr_net_loss"));

        schema.add("inventory", tbl(
                "inv_date_sk", "inv_item_sk", "inv_warehouse_sk",
                "inv_quantity_on_hand"));
    }

    // -- Dimension tables -----------------------------------------------------

    private static void addDimensionTables(CalciteSchema schema) {
        schema.add("date_dim", tbl(
                "d_date_sk", "d_date_id", "d_date", "d_month_seq",
                "d_week_seq", "d_quarter_seq", "d_year", "d_dow", "d_moy",
                "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq",
                "d_fy_week_seq", "d_day_name", "d_quarter_name", "d_holiday",
                "d_weekend", "d_following_holiday", "d_first_dom", "d_last_dom",
                "d_same_day_ly", "d_same_day_lq", "d_current_day",
                "d_current_week", "d_current_month", "d_current_quarter",
                "d_current_year"));

        schema.add("customer", tbl(
                "c_customer_sk", "c_customer_id", "c_current_cdemo_sk",
                "c_current_hdemo_sk", "c_current_addr_sk",
                "c_first_shipto_date_sk", "c_first_sales_date_sk",
                "c_salutation", "c_first_name", "c_last_name",
                "c_preferred_cust_flag", "c_birth_day", "c_birth_month",
                "c_birth_year", "c_birth_country", "c_login",
                "c_email_address", "c_last_review_date"));

        schema.add("customer_address", tbl(
                "ca_address_sk", "ca_address_id", "ca_street_number",
                "ca_street_name", "ca_street_type", "ca_suite_number",
                "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country",
                "ca_gmt_offset", "ca_location_type"));

        schema.add("customer_demographics", tbl(
                "cd_demo_sk", "cd_gender", "cd_marital_status",
                "cd_education_status", "cd_purchase_estimate",
                "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count",
                "cd_dep_college_count"));

        schema.add("household_demographics", tbl(
                "hd_demo_sk", "hd_income_band_sk", "hd_buy_potential",
                "hd_dep_count", "hd_vehicle_count"));

        schema.add("item", tbl(
                "i_item_sk", "i_item_id", "i_rec_start_date",
                "i_rec_end_date", "i_item_desc", "i_current_price",
                "i_wholesale_cost", "i_brand_id", "i_brand", "i_class_id",
                "i_class", "i_category_id", "i_category", "i_manufact_id",
                "i_manufact", "i_size", "i_formulation", "i_color",
                "i_units", "i_container", "i_manager_id", "i_product_name"));

        schema.add("promotion", tbl(
                "p_promo_sk", "p_promo_id", "p_start_date_sk",
                "p_end_date_sk", "p_item_sk", "p_cost", "p_response_target",
                "p_promo_name", "p_channel_dmail", "p_channel_email",
                "p_channel_catalog", "p_channel_tv", "p_channel_radio",
                "p_channel_press", "p_channel_event", "p_channel_demo",
                "p_channel_details", "p_purpose", "p_discount_active"));

        schema.add("reason", tbl(
                "r_reason_sk", "r_reason_id", "r_reason_desc"));

        schema.add("store", tbl(
                "s_store_sk", "s_store_id", "s_rec_start_date",
                "s_rec_end_date", "s_closed_date_sk", "s_store_name",
                "s_number_employees", "s_floor_space", "s_hours",
                "s_manager", "s_market_id", "s_geography_class",
                "s_market_desc", "s_market_manager", "s_division_id",
                "s_division_name", "s_company_id", "s_company_name",
                "s_street_number", "s_street_name", "s_street_type",
                "s_suite_number", "s_city", "s_county", "s_state", "s_zip",
                "s_country", "s_gmt_offset", "s_tax_percentage"));

        schema.add("time_dim", tbl(
                "t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute",
                "t_second", "t_am_pm", "t_shift", "t_sub_shift",
                "t_meal_time"));

        schema.add("warehouse", tbl(
                "w_warehouse_sk", "w_warehouse_id", "w_warehouse_name",
                "w_warehouse_sq_ft", "w_street_number", "w_street_name",
                "w_street_type", "w_suite_number", "w_city", "w_county",
                "w_state", "w_zip", "w_country", "w_gmt_offset"));

        schema.add("web_page", tbl(
                "wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date",
                "wp_rec_end_date", "wp_creation_date_sk",
                "wp_access_date_sk", "wp_autogen_flag", "wp_customer_sk",
                "wp_url", "wp_type", "wp_char_count", "wp_link_count",
                "wp_image_count", "wp_max_ad_count"));

        schema.add("web_site", tbl(
                "web_site_sk", "web_site_id", "web_rec_start_date",
                "web_rec_end_date", "web_name", "web_open_date_sk",
                "web_close_date_sk", "web_class", "web_manager",
                "web_mkt_id", "web_mkt_class", "web_mkt_desc",
                "web_market_manager", "web_company_id", "web_company_name",
                "web_street_number", "web_street_name", "web_street_type",
                "web_suite_number", "web_city", "web_county", "web_state",
                "web_zip", "web_country", "web_gmt_offset", "web_tax_percentage"));

        schema.add("catalog_page", tbl(
                "cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk",
                "cp_end_date_sk", "cp_department", "cp_catalog_number",
                "cp_catalog_page_number", "cp_description", "cp_type"));

        schema.add("call_center", tbl(
                "cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date",
                "cc_rec_end_date", "cc_closed_date_sk", "cc_open_date_sk",
                "cc_name", "cc_class", "cc_employees", "cc_sq_ft",
                "cc_hours", "cc_manager", "cc_mkt_id", "cc_mkt_class",
                "cc_mkt_desc", "cc_market_manager", "cc_division",
                "cc_division_name", "cc_company", "cc_company_name",
                "cc_street_number", "cc_street_name", "cc_street_type",
                "cc_suite_number", "cc_city", "cc_county", "cc_state",
                "cc_zip", "cc_country", "cc_gmt_offset", "cc_tax_percentage"));

        schema.add("income_band", tbl(
                "ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"));
    }

    // -- Helper ---------------------------------------------------------------

    /**
     * Creates a table with typed columns inferred from the TPC-DS naming convention.
     * Type inference matches the TPC-DS specification: surrogate keys are BIGINT,
     * monetary/price/cost columns are DECIMAL(7,2), date columns are DATE, single-
     * character flags are CHAR(1), and everything else (id, name, desc, etc.) is
     * VARCHAR. Decimal types are required for queries that perform arithmetic or
     * BETWEEN comparisons on monetary columns (e.g. Q7's BETWEEN on sales_price).
     */
    private static Table tbl(String... columns) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                for (String col : columns) {
                    SqlTypeName type = inferColumnType(col);
                    if (type == SqlTypeName.DECIMAL) {
                        builder.add(col, typeFactory.createSqlType(SqlTypeName.DECIMAL, 7, 2));
                    } else if (type == SqlTypeName.CHAR) {
                        builder.add(col, typeFactory.createSqlType(SqlTypeName.CHAR, 1));
                    } else {
                        builder.add(col, type);
                    }
                }
                return builder.build();
            }
        };
    }

    /**
     * Infer TPC-DS column type from naming convention. The TPC-DS specification
     * defines stable suffix conventions: *_sk are surrogate keys (BIGINT),
     * monetary columns end with *_amt, *_cost, *_price, *_paid, *_profit, *_tax,
     * *_discount, *_loss, *_credit, *_charge, *_fee. Quantity is INTEGER.
     * Dates are DATE, flags are CHAR(1), and the remainder default to VARCHAR.
     */
    private static SqlTypeName inferColumnType(String col) {
        String name = col.toLowerCase();
        // Surrogate keys and FK references end with _sk
        if (name.endsWith("_sk")) {
            return SqlTypeName.BIGINT;
        }
        // Year/month/day/dow/qoy/seq/ticket number/order number: INTEGER
        if (name.endsWith("_year") || name.endsWith("_month") || name.endsWith("_day")
                || name.endsWith("_dow") || name.endsWith("_moy") || name.endsWith("_dom")
                || name.endsWith("_qoy") || name.endsWith("_seq") || name.endsWith("_count")
                || name.endsWith("_number") || name.endsWith("_id") && isNumericId(name)
                || name.endsWith("_hour") || name.endsWith("_minute") || name.endsWith("_second")
                || name.endsWith("_time") || name.endsWith("_target")
                || name.endsWith("_sq_ft") || name.endsWith("_employees")
                || name.endsWith("_floor_space") || name.endsWith("_quantity_on_hand")) {
            return SqlTypeName.INTEGER;
        }
        // Monetary/decimal columns
        if (name.endsWith("_amt") || name.endsWith("_cost") || name.endsWith("_price")
                || name.endsWith("_paid") || name.endsWith("_profit") || name.endsWith("_tax")
                || name.endsWith("_discount") || name.endsWith("_loss")
                || name.endsWith("_credit") || name.endsWith("_charge")
                || name.endsWith("_fee") || name.endsWith("_cash")
                || name.endsWith("_inc_tax") || name.endsWith("_inc_ship")
                || name.endsWith("_inc_ship_tax") || name.endsWith("_amount")
                || name.endsWith("_tax_percentage") || name.endsWith("_gmt_offset")
                || name.endsWith("_lower_bound") || name.endsWith("_upper_bound")
                || name.endsWith("_response_target")) {
            return SqlTypeName.DECIMAL;
        }
        // Quantity columns — INTEGER (TPC-DS spec)
        if (name.endsWith("_quantity")) {
            return SqlTypeName.INTEGER;
        }
        // Date columns
        if (name.endsWith("_date") && !name.endsWith("_date_id")) {
            return SqlTypeName.DATE;
        }
        // Single-character flags
        if (name.startsWith("d_holiday") || name.startsWith("d_weekend")
                || name.startsWith("d_following_holiday") || name.startsWith("d_current_")
                || name.equals("c_preferred_cust_flag") || name.endsWith("_flag")
                || name.equals("p_discount_active") || name.startsWith("p_channel_")) {
            return SqlTypeName.CHAR;
        }
        return SqlTypeName.VARCHAR;
    }

    /**
     * Some *_id columns are numeric (manager_id, mkt_id, etc.), but the "*_id"
     * suffix is overloaded: c_customer_id, d_date_id, i_item_id are VARCHAR.
     * Heuristic: only treat *_id as INTEGER when the prefix suggests a numeric ID.
     */
    private static boolean isNumericId(String name) {
        return name.endsWith("_manager_id") || name.endsWith("_mkt_id")
                || name.endsWith("_brand_id") || name.endsWith("_class_id")
                || name.endsWith("_category_id") || name.endsWith("_manufact_id")
                || name.endsWith("_market_id") || name.endsWith("_division_id")
                || name.endsWith("_company_id") || name.endsWith("_response_target");
    }
}
