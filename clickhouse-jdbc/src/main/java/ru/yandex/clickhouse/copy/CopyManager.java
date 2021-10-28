package ru.yandex.clickhouse.copy;

import java.io.*;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface CopyManager extends AutoCloseable {

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(sql, sourceInputStream);
     *     }
     * </pre>
     *
     * @param sql - SQL INSERT Query
     * @param from - InputStream with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, InputStream from) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Path path = ...;
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(sql, path);
     *     }
     * </pre>
     *
     * @param sql - SQL INSERT Query
     * @param path - Path to the file with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, Path path) throws IOException, SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     File file = ...;
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(sql, file);
     *     }
     * </pre>
     *
     * @param sql - SQL INSERT Query
     * @param file - Path to the file with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, File file) throws IOException, SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(sql, sourceInputStream, 1024);
     *     }
     * </pre>
     *
     * @param sql - SQL INSERT Query
     * @param from - InputStream with data
     * @param bufferSize - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, InputStream from, int bufferSize) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(sql, sourceReader);
     *     }
     * </pre>
     *
     * @param sql  - SQL INSERT Query
     * @param from - Reader with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, Reader from) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(sql, sourceReader);
     *     }
     * </pre>
     *
     * @param sql  - SQL INSERT Query
     * @param from - Reader with data
     * @param bufferSize - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, Reader from, int bufferSize) throws SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(sql, fos);
     *     }
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to - OutputStream to write data
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql, OutputStream to) throws SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(sql, fos);
     *     }
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to - Writer to write the data to
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql, Writer to) throws SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the file
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     Path path = ...;
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(sql, path);
     *     }
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to - OutputStream to write data
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql, Path to) throws IOException, SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the file
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     File file = ...;
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(sql, file);
     *     }
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to - OutputStream to write data
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql, File to) throws IOException, SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table where group_id = ? FORMAT CSV";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(ps, sourceInputStream);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL INSERT Query
     * @param from              - InputStream with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(PreparedStatement preparedStatement, InputStream from) throws SQLException;

    /**
     * Inserts the data from the path to the DB.
     * The type and structure of the data from the path specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Path path = ...;
     *     String sql = "INSERT INTO default.my_table where group_id = ? FORMAT CSV";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(ps, path);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL INSERT Query
     * @param path              - Path to the file with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(PreparedStatement preparedStatement, Path path) throws IOException, SQLException;

    /**
     * Inserts the data from the file to the DB.
     * The type and structure of the data from the file specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     File file = ...;
     *     String sql = "INSERT INTO default.my_table where group_id = ? FORMAT CSV";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(ps, file);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL INSERT Query
     * @param file              - Path to the file with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(PreparedStatement preparedStatement, File file) throws IOException, SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table where group_id = ? FORMAT CSV";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(ps, sourceInputStream, 1024);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL INSERT Query
     * @param from              - InputStream with data
     * @param bufferSize        - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(PreparedStatement preparedStatement, InputStream from, int bufferSize) throws SQLException;

    /**
     * Inserts the data from the reader to the DB.
     * The type and structure of the data from the reader specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table where group_id = ? FORMAT CSV";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(ps, sourceReader);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL INSERT Query
     * @param from              - Reader with data
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(PreparedStatement preparedStatement, Reader from) throws SQLException;

    /**
     * Inserts the data from the reader to the DB.
     * The type and structure of the data from the reader specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table where group_id = ? FORMAT CSV";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyToDb(ps, sourceReader, 1024);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL INSERT Query
     * @param from              - Reader with data
     * @param bufferSize        - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(PreparedStatement preparedStatement, Reader from, int bufferSize) throws SQLException;

    /**
     * Loads the data from the DB using the PreparedStatement and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table where group_id = ? FORMAT CSVWithNames";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(ps, fos);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL and params
     * @param to                - OutputStream to write data
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(PreparedStatement preparedStatement, OutputStream to) throws SQLException;

    /**
     * Loads the data from the DB using the PreparedStatement and writes it to the writer
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     Writer writer = ...
     *     String sql = "SELECT * from default.my_table where group_id = ? FORMAT CSVWithNames";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(ps, writer);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL and params
     * @param to                - Writer to write the data to
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(PreparedStatement preparedStatement, Writer to) throws SQLException;

    /**
     * Loads the data from the DB using the PreparedStatement and writes it to the file
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     Path path = ...;
     *     String sql = "SELECT * from default.my_table where group_id = ? FORMAT CSVWithNames";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(ps, path);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL and params
     * @param to                - Path to write data
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(PreparedStatement preparedStatement, Path to) throws IOException, SQLException;

    /**
     * Loads the data from the DB using the PreparedStatement and writes it to the file
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     File file = ...;
     *     String sql = "SELECT * from default.my_table where group_id = ? FORMAT CSVWithNames";
     *     PreparedStatement ps = connection.prepareStatement(sql);
     *     ps.setLong(1, 10);
     *     try (CopyManager copyManager = CopyManagerFactory.create(connection)) {
     *         copyManager.copyFromDb(ps, file);
     *     }
     * </pre>
     *
     * @param preparedStatement - PreparedStatement with SQL and params
     * @param to                - File to write data
     * @throws SQLException - loading error
     * @see ru.yandex.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(PreparedStatement preparedStatement, File to) throws IOException, SQLException;
}
