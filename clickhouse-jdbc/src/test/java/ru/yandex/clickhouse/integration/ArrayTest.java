package ru.yandex.clickhouse.integration;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.*;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.response.ClickHouseResultSet;
import ru.yandex.clickhouse.response.ClickHouseResultSetMetaData;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import sun.misc.BASE64Decoder;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.testng.Assert.assertEquals;

/**
 * Here it is assumed the connection to a ClickHouse instance with flights example data it available at localhost:8123
 * For ClickHouse quickstart and example dataset see <a href="https://clickhouse.yandex/tutorial.html">https://clickhouse.yandex/tutorial.html</a>
 */
public class ArrayTest {

    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
//        dataSource = ClickHouseContainerForTest.newDataSource();
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("default");
//        dataSource = ClickHouseContainerForTest.newDataSource("jdbc:clickhouse://10.218.1.204:8123/default");
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.218.1.204:8123/default", props);
//        dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.218.1.205:8123/default", props);
//        dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.218.1.206:8123/default", props);
        connection = dataSource.getConnection();

    }


    @AfterTest
    public void shutdown() throws Exception {
        connection.close();
        System.out.println("test--");
    }



    @Test
    public void testBit() throws Exception {
        BASE64Decoder base64Decoder = new BASE64Decoder();
        byte[] bytes = base64Decoder.decodeBuffer("\\x3a300000010000000000020010000000010002000300");
        System.out.println(bytes);
//        System.out.println(new String(bytes));
//
//        MutableRoaringBitmap b = new MutableRoaringBitmap();
//        b.deserialize(ByteBuffer.wrap(bytes));
//
//        System.out.println(b.contains(1));

        RoaringBitmap b = RoaringBitmap.bitmapOf(1, 2, 3);


    }



    @Test
    public void testInsertShard() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("default");


        String dropSql = "DROP TABLE IF EXISTS default.test_array";
        String sql = "CREATE TABLE IF NOT EXISTS default.test_array (s_arr Array(Nullable(String)), " +
                "dt_arr Array(Nullable(DateTime64)), d_arr Array(Nullable(Date)), ui_arr Array(Nullable(UInt8)), i_arr Array(Int64), " +
                "di_arr Array(Decimal64(6)), f_arr Array(Float64), n Nullable(Int8), s Array(Nullable(String)) " +
//                "rb AggregateFunction(groupBitmap, UInt32)) ENGINE = TinyLog");
                ") ENGINE = TinyLog";
        String insertSQL = "INSERT INTO default.test_array VALUES (?,?,?,?,?,?,?,?,?)";

        dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.218.1.204:8123/default", props);
        connection = dataSource.getConnection();
        connection.createStatement().execute(dropSql);
        connection.createStatement().execute(sql);

        ClickHousePreparedStatementImpl statement = (ClickHousePreparedStatementImpl) connection.prepareStatement(insertSQL);
        statement.addRow("['{\"204_sender\":\"pablo\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"204_sender\":\"aa\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"204_sender\":\"dd\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"204_sender\":\"bb\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.executeBatch();
        connection.close();


        dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.218.1.205:8123/default", props);
        connection = dataSource.getConnection();
        connection.createStatement().execute(dropSql);
        connection.createStatement().execute(sql);

        statement = (ClickHousePreparedStatementImpl) connection.prepareStatement(insertSQL);
        statement.addRow("['{\"205_sender\":\"pablo\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"205_sender\":\"aa\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"205_sender\":\"dd\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"205_sender\":\"bb\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.executeBatch();
        connection.close();



        dataSource = new ClickHouseDataSource("jdbc:clickhouse://10.218.1.206:8123/default", props);
        connection = dataSource.getConnection();
        connection.createStatement().execute(dropSql);
        connection.createStatement().execute(sql);

        statement = (ClickHousePreparedStatementImpl) connection.prepareStatement(insertSQL);
        statement.addRow("['{\"206_sender\":\"pablo\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"206_sender\":\"aa\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"206_sender\":\"dd\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"206_sender\":\"bb\",\"body\":\"they are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.executeBatch();
    }



    @Test
    public void testInsertRowString() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS default.test_array");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS default.test_array (s_arr Array(Nullable(String)), " +
                "dt_arr Array(Nullable(DateTime64)), d_arr Array(Nullable(Date)), ui_arr Array(Nullable(UInt8)), i_arr Array(Int64), " +
                "di_arr Array(Decimal64(6)), f_arr Array(Float64), n Nullable(Int8), s Array(Nullable(String)) " +
//                "rb AggregateFunction(groupBitmap, UInt32)) ENGINE = TinyLog");
                ") ENGINE = TinyLog");

        String insertSQL = "INSERT INTO default.test_array VALUES (?,?,?,?,?,?,?,?,?)";
        ClickHousePreparedStatementImpl statement = (ClickHousePreparedStatementImpl) connection.prepareStatement(insertSQL);
        statement.addRow("['{\"sender\":\"pablo\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t\\N");
        statement.addRow("['{\"sender\":\"aa\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"sender\":\"dd\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.addRow("['{\"sender\":\"bb\",\"body\":\"they \\'are \\\\\"on to us\"}',NULL,'{\"sender\":\"arthur\"}']\t['2019-01-01 00:00:00.456',NULL,'2019-01-03 00:00:00.789']\t['2019-01-01','2019-01-03',NULL]\t[1,0,NULL]\t[234243,453424]\t[234243.333,453424.456]\t[234243.333,453424.456]\t\\N\t['aa,f',NULL,'c\"cc','dd']");
        statement.executeBatch();
    }

    private Map<String, ColumnInfo> columnInfoMap = new ConcurrentHashMap<>();
    private class ColumnInfo {
        public String columnType;
        public Integer columnIndex;
        public ColumnInfo(Integer columnIndex, String columnType) {
            this.columnType = columnType;
            this.columnIndex = columnIndex;
        }
    }

    @Test
    public void testInsertDateTime() throws Exception {
//        connection.createStatement().execute("DROP TABLE IF EXISTS default.string_array");
//        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS default.string_array (foo Array(String)) ENGINE = TinyLog");
        connection.createStatement().execute("DROP TABLE IF EXISTS default.test_array");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS default.test_array (s_arr Array(Nullable(String)), " +
                "dt_arr Array(Nullable(DateTime64)), d_arr Array(Nullable(Date)), ui_arr Array(Nullable(UInt8)), i_arr Array(Int64), " +
                "di_arr Array(Decimal64(6)), f_arr Array(Float64), n Nullable(Int8), s Array(Nullable(String)) " +
                ",dt Nullable(DateTime64), str Nullable(String)" +
//                "rb AggregateFunction(groupBitmap, UInt32)) ENGINE = TinyLog");
                ") ENGINE = TinyLog");

        String insertSQL = "INSERT INTO default.test_array VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setArray(1, connection.createArrayOf("String", new Object[]{"{\"sender\":\"pablo\",\"body\":\"they 'are \\\"on to us\"}", null, "{\"sender\":\"arthur\"}"}));
        statement.setArray(2, connection.createArrayOf("String", new Object[]{"2019-01-01 00:00:00.456", null, "2019-01-03 00:00:00.789"}));
        statement.setArray(3, connection.createArrayOf("String", new Object[]{"2019-01-01", "2019-01-03", "NULL"}));
//        statement.setNull(3, Types.ARRAY);
//        statement.setArray(4, connection.createArrayOf("Decimal", new Object[]{"1", "0", "null"}));
        statement.setArray(4, connection.createArrayOf("Decimal", new Object[]{true, false, "null"}));
        statement.setArray(5, connection.createArrayOf("Decimal", new Object[]{"234243", "453424"}));
        statement.setArray(6, connection.createArrayOf("Decimal", new Object[]{"234243.333", "453424.456"}));
        statement.setArray(7, connection.createArrayOf("Decimal", new Object[]{"234243.333", "453424.456"}));
        statement.setNull(8, Types.BOOLEAN);
        statement.setArray(9, connection.createArrayOf("String", new Object[]{"aa,f", null, "c\"cc", "dd"}));
//        statement.setObject(10, "\\x3a300000010000000000020010000000010002000300".getBytes());
        statement.setString(10, "2019-01-01 00:00:00.456");
        statement.setString(11, "{\"sender\":\"pablo\",\"body\":\"they are on \\\"to us\"}");
        statement.addBatch();


        statement.setArray(1, connection.createArrayOf("String", new Object[]{"{\"sender\":\"pablo\",\"body\":\"they 'are \\\"on to us\"}", null, "{\"sender\":\"arthur\"}"}));
//        statement.setNull(2, Types.ARRAY);
        statement.setArray(2, connection.createArrayOf("String", new Object[]{"2019-01-01 00:00:00.456", null, "2019-01-04 00:00:00.456"}));
        statement.setArray(3, connection.createArrayOf("String", new Object[]{"2019-01-01", "2019-01-03"}));
//        statement.setArray(3, connection.createArrayOf("String", new Object[]{"2019-01-01", "2019-01-03", "null"}));
//        statement.setNull(3, Types.ARRAY);
        statement.setArray(4, connection.createArrayOf("Decimal", new Object[]{"1", "0", "null"}));
//        statement.setArray(4, connection.createArrayOf("String", new String[]{"true", "false", "null"}));
        statement.setArray(5, connection.createArrayOf("Decimal", new Object[]{"234243", "453424"}));
        statement.setArray(6, connection.createArrayOf("Decimal", new Object[]{"234243.333", "453424.456"}));
        statement.setArray(7, connection.createArrayOf("Decimal", new Object[]{"234243.333", "453424.456"}));
        statement.setBoolean(8, false);
        statement.setArray(9, connection.createArrayOf("String", new Object[]{"aa,f", null, "c\"cc", "dd"}));
//        statement.setObject(10, "\\x3a300000010000000000020010000000010002000300".getBytes());
        statement.setString(10, "2019-01-01 00:00:00.456");
        statement.setString(11, "asdf6");
        statement.addBatch();

        statement.setArray(1, connection.createArrayOf("String", new Object[]{"{\"sender\":\"dd\",\"body\":\"they 'are \\\"on to us\"}", null, "{\"sender\":\"arthur\"}"}));
        statement.setArray(2, connection.createArrayOf("String", new Object[]{"2019-01-02 00:00:00.456", null, "2019-01-03 00:00:00.789"}));
//        statement.setArray(3, connection.createArrayOf("String", new String[]{"2019-01-01", "2019-01-03"}));
        statement.setArray(3, connection.createArrayOf("String", new Object[]{"2019-01-02", "2019-01-03", "null"}));
//        statement.setNull(3, Types.ARRAY);
        statement.setArray(4, connection.createArrayOf("Decimal", new Object[]{"1", "0", "null"}));
//        statement.setArray(4, connection.createArrayOf("String", new String[]{"true", "false", "null"}));
        statement.setArray(5, connection.createArrayOf("Decimal", new Object[]{"223423", "2342"}));
        statement.setArray(6, connection.createArrayOf("Decimal", new Object[]{"223423.333", "2342.456"}));
        statement.setArray(7, connection.createArrayOf("Decimal", new Object[]{"223423.333", "2342.456"}));
        statement.setNull(8, Types.BOOLEAN);
        statement.setArray(9, connection.createArrayOf("String", new Object[]{"aaf,wee'", null, "c\"cc", "dd"}));
//        statement.setObject(10, "\\x3a300000010000000000020010000000010002000300".getBytes());
        statement.setString(10, "2019-01-01 00:00:00.456");
        statement.setString(11, "asdf6");
        statement.addBatch();


        statement.executeBatch();

        ClickHouseResultSet r = (ClickHouseResultSet)connection.createStatement().executeQuery(
                "SELECT d_arr FROM default.test_array");
//        unwrap.findColumn()
        List<ClickHouseColumnInfo> columns = r.getColumns();

        int index = 0;
        for (ClickHouseColumnInfo column : columns) {
            column.getClickHouseDataType().getSqlType();
            columnInfoMap.put(column.getColumnName(), new ColumnInfo(++index, ""));
        }

//        resultSet.getColumn

        if (columns.get(0).isArray()) {
            JDBCType jdbcType = columns.get(0).getArrayBaseType().getJdbcType();



        }

        r.next();

//        Object[] s1 = (Object[]) r.getArray(1).getArray();
//        Object[] s2 = (Object[]) r.getArray(2).getArray();
//        Object[] s3 = (Object[]) r.getArray(3).getArray();
//        int[] s4 = (int[]) r.getArray(4).getArray();
//        long[] s5 = (long[]) r.getArray(5).getArray();
//        Object[] s6 = (Object[]) r.getArray(6).getArray();
//        double[] s7 = (double[]) r.getArray(7).getArray();
//        boolean r8 = r.getBoolean(8);
        String s1 = r.getString(1);
        String s2 = r.getString(2);
        String s3 = r.getString(3);
        String s4 = r.getString(4);
        String s5 = r.getString(5);
        String s6 = r.getString(6);
        String s7 = r.getString(7);
        boolean r8 = r.getBoolean(8);
        String s9 = r.getString(9);
        String s10 = r.getString(10);
        String s11 = r.getString(11);


        System.out.println(s1 + "\t" + s2 + "\t" + s3 + "\t" + s4 + "\t" + s5 + "\t" + s6 + "\t" + s7 + "\t" + r8 + "\t"
                + s9 + "\t" + s10 + "\t" + s11);

        System.out.println("------------");

        String[] valsj = (String[]) r.getArray(1).getArray();
        StringBuffer resultStrj = new StringBuffer("{");
        for (String item : valsj) {
            if (item != null) {
                if (item.contains("\\")) {
                    item = item.replaceAll("\\\\", "\\\\\\\\");
                }
                resultStrj.append("\"").append(item.replaceAll("\"", "\\\\\"")).append("\"").append(",");
            } else {
                resultStrj.append("NULL").append(",");
            }
        }
        resultStrj.deleteCharAt(resultStrj.length() - 1);
        System.out.println(resultStrj.append("}").toString());

        Date[] vals = (Date[]) r.getArray(3).getArray();
        StringBuffer resultStr = new StringBuffer("{");
        for (Date item : vals) {
            resultStr.append(item == null ? "NULL" : item).append(",");
        }
        resultStr.deleteCharAt(resultStr.length() - 1);
        System.out.println(resultStr.append("}").toString());

        Timestamp[] valst = (Timestamp[]) r.getArray(2).getArray();
        StringBuffer resultStrt = new StringBuffer("{");
        for (Timestamp item : valst) {
            if (item != null) {
                resultStrt.append("\"").append(item).append("\"").append(",");
            } else {
                resultStrt.append("NULL").append(",");
            }
        }
        resultStrt.deleteCharAt(resultStrt.length() - 1);
        System.out.println(resultStrt.append("}").toString());


        int[] valsb = (int[]) r.getArray(4).getArray();
        StringBuffer resultStrb = new StringBuffer("{");
        for (int item : valsb) {
//            if (item != null) {
            resultStrb.append(item == 1 ? "t" : "f").append(",");
//            } else {
//                resultStrb.append("NULL").append(",");
//            }
        }
        resultStrb.deleteCharAt(resultStrb.length() - 1);
        System.out.println(resultStrb.append("}").toString());


        String[] vals9 = (String[]) r.getArray(9).getArray();
        StringBuffer resultStrs9 = new StringBuffer("{");
        for (String item : vals9) {
            if (item != null) {
//                if (item.contains("\\")) {
//                    item = item.replaceAll("\\\\","\\\\\\\\");
//                }
//                resultStrs9.append("\"").append(item.replaceAll("\"", "\\\\\"")).append("\"").append(",");
                resultStrs9.append("\"").append(item).append("\"").append(",");
            } else {
                resultStrs9.append("NULL").append(",");
            }
        }
        resultStrs9.deleteCharAt(resultStrs9.length() - 1);
        System.out.println(resultStrs9.append("}").toString());


    }


    @Test
    public void testStringArray() throws SQLException {
        String[] array = {"a'','sadf',aa", "", ",", "юникод,'юникод'", ",2134,saldfk"};

        StringBuilder sb = new StringBuilder();
        for (String s : array) {
            sb.append("','").append(s.replace("'", "\\'"));
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(0).deleteCharAt(0).append('\'');
        }

        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.VARCHAR);
            String[] stringArray = (String[]) rs.getArray(1).getArray();
            assertEquals(stringArray.length, array.length);
            for (int i = 0; i < stringArray.length; i++) {
                assertEquals(stringArray[i], array[i]);
            }
        }
        statement.close();
    }

    @Test
    public void testLongArray() throws SQLException {
        Long[] array = {-12345678987654321L, 23325235235L, -12321342L};
        StringBuilder sb = new StringBuilder();
        for (long l : array) {
            sb.append("),toInt64(").append(l);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0).deleteCharAt(0).append(')');
        }
        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.BIGINT);
            long[] longArray = (long[]) rs.getArray(1).getArray();
            assertEquals(longArray.length, array.length);
            for (int i = 0; i < longArray.length; i++) {
                assertEquals(longArray[i], array[i].longValue());
            }
        }
        statement.close();
    }

    @Test
    public void testDecimalArray() throws SQLException {
        BigDecimal[] array = {BigDecimal.valueOf(-12.345678987654321), BigDecimal.valueOf(23.325235235), BigDecimal.valueOf(-12.321342)};
        StringBuilder sb = new StringBuilder();
        for (BigDecimal d : array) {
            sb.append(", 15),toDecimal64(").append(d);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0).delete(0, sb.indexOf(",") + 1).append(", 15)");
        }
        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.DECIMAL);
            BigDecimal[] deciamlArray = (BigDecimal[]) rs.getArray(1).getArray();
            assertEquals(deciamlArray.length, array.length);
            for (int i = 0; i < deciamlArray.length; i++) {
                assertEquals(0, deciamlArray[i].compareTo(array[i]));
            }
        }
        statement.close();
    }

    @Test
    public void testInsertUIntArray() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.unsigned_array");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.unsigned_array"
                        + " (ua32 Array(UInt32), ua64 Array(UInt64), f64 Array(Float64), a32 Array(Int32)) ENGINE = TinyLog"
        );

        String insertSql = "INSERT INTO test.unsigned_array (ua32, ua64, f64, a32) VALUES (?, ?, ?, ?)";

        PreparedStatement statement = connection.prepareStatement(insertSql);

        statement.setArray(1, new ClickHouseArray(ClickHouseDataType.UInt64, new long[]{4294967286L, 4294967287L}));
        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.UInt64, new BigInteger[]{new BigInteger("18446744073709551606"), new BigInteger("18446744073709551607")}));
        statement.setArray(3, new ClickHouseArray(ClickHouseDataType.Float64, new double[]{1.23, 4.56}));
        statement.setArray(4, new ClickHouseArray(ClickHouseDataType.Int32, new int[]{-2147483648, 2147483647}));
        statement.execute();

        statement = connection.prepareStatement(insertSql);

        statement.setObject(1, new ArrayList<Object>(Arrays.asList(4294967286L, 4294967287L)));
        statement.setObject(2, new ArrayList<Object>(Arrays.asList(
                new BigInteger("18446744073709551606"),
                new BigInteger("18446744073709551607"))));
        statement.setObject(3, new ArrayList<Object>(Arrays.asList(1.23, 4.56)));
        statement.setObject(4, Arrays.asList(-2147483648, 2147483647));
        statement.execute();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ua32, ua64, f64, a32 from test.unsigned_array");
        for (int i = 0; i < 2; ++i) {
            rs.next();
            Array bigUInt32 = rs.getArray(1);
            Assert.assertEquals(bigUInt32.getBaseType(), Types.BIGINT); //
            Assert.assertEquals(bigUInt32.getArray().getClass(), long[].class);
            Assert.assertEquals(((long[]) bigUInt32.getArray())[0], 4294967286L);
            Assert.assertEquals(((long[]) bigUInt32.getArray())[1], 4294967287L);
            Array bigUInt64 = rs.getArray(2);
            Assert.assertEquals(bigUInt64.getBaseType(), Types.BIGINT);
            Assert.assertEquals(bigUInt64.getArray().getClass(), BigInteger[].class);
            Assert.assertEquals(((BigInteger[]) bigUInt64.getArray())[0], new BigInteger("18446744073709551606"));
            Assert.assertEquals(((BigInteger[]) bigUInt64.getArray())[1], new BigInteger("18446744073709551607"));
            Array float64 = rs.getArray(3);
            Assert.assertEquals(float64.getBaseType(), Types.DOUBLE);
            Assert.assertEquals(float64.getArray().getClass(), double[].class);
            Assert.assertEquals(((double[]) float64.getArray())[0], 1.23, 0.0000001);
            Assert.assertEquals(((double[]) float64.getArray())[1], 4.56, 0.0000001);
            Array int32 = rs.getArray(4);
            Assert.assertEquals(int32.getBaseType(), Types.INTEGER); //
            Assert.assertEquals(int32.getArray().getClass(), int[].class);
            Assert.assertEquals(((int[]) int32.getArray())[0], -2147483648);
            Assert.assertEquals(((int[]) int32.getArray())[1], 2147483647);
        }
    }

    @Test
    public void testInsertStringArray() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.string_array");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.string_array (foo Array(String)) ENGINE = TinyLog");

        String insertSQL = "INSERT INTO test.string_array (foo) VALUES (?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setArray(1, connection.createArrayOf(
                String.class.getCanonicalName(),
                new String[]{"23", "42"}));
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
                "SELECT foo FROM test.string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }

    @Test
    public void testInsertStringArrayViaUnwrap() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.string_array");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.string_array (foo Array(String)) ENGINE = TinyLog");

        String insertSQL = "INSERT INTO test.string_array (foo) VALUES (?)";
        ClickHousePreparedStatement statement = connection.prepareStatement(insertSQL)
                .unwrap(ClickHousePreparedStatement.class);
        statement.setArray(1, new String[]{"23", "42"});
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
                "SELECT foo FROM test.string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }
}
