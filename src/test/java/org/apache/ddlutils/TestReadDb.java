package org.apache.ddlutils;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.mssql.MSSqlModelReader;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @auther:zh
 * @date:2019/1/28
 */
public class TestReadDb {


    @Test
    public void testMssql() throws SQLException {
        BasicDataSource  dataSource=new BasicDataSource();
        dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSource.setUrl("jdbc:sqlserver://ldwqh0.oicp.net:14330;databaseName=cqtbtest");
        dataSource.setUsername("sa");
        dataSource.setPassword("admin123456!@#");
        final Platform sourcePlatform = PlatformFactory.createNewPlatformInstance(dataSource);
        sourcePlatform.setDelimitedIdentifierModeOn(true);
        final Database sourceDatabase = sourcePlatform.readModelFromDatabase("cqtbtest", "cqtbtest", "dbo", null);
        {
            //new DatabaseIO().write(sourceDatabase, "database.ddl");
            final Table[] tables = sourceDatabase.getTables();
            for (final Table table : tables) {
                System.out.println(table.getName() + " : " + table.getDescription());

                final Column[] columns = table.getColumns();
                for (final Column column : columns) {
                    System.out.print(column.getName() + "[" + column.getDescription() + "], ");
                }
                System.out.println();
            }
        }
    }
    @Test
    public void testMysql() {
        BasicDataSource  dataSource=new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        final Platform sourcePlatform = PlatformFactory.createNewPlatformInstance(dataSource);
        sourcePlatform.setDelimitedIdentifierModeOn(true);
        final Database sourceDatabase = sourcePlatform.readModelFromDatabase("test", "test", "test", null);
        {
            //new DatabaseIO().write(sourceDatabase, "database.ddl");
            final Table[] tables = sourceDatabase.getTables();
            for (final Table table : tables) {
                System.out.println(table.getName() + " : " + table.getDescription());

                final Column[] columns = table.getColumns();
                for (final Column column : columns) {
                    System.out.print(column.getName() + "[" + column.getDescription() + "], ");
                }
                System.out.println();
            }
        }
    }






}
