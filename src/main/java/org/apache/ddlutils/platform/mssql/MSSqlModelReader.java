package org.apache.ddlutils.platform.mssql;

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

import java.sql.*;
import java.sql.Date;
import java.text.Collator;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.JdbcModelReader;

/**
 * Reads a database model from a Microsoft Sql Server database.
 *
 * @version $Revision: $
 */
public class MSSqlModelReader extends JdbcModelReader
{
    /** Known system tables that Sql Server creates (e.g. automatic maintenance). */
    private static final String[] KNOWN_SYSTEM_TABLES = { "dtproperties" };
	/** The regular expression pattern for the ISO dates. */
	private Pattern _isoDatePattern;
	/** The regular expression pattern for the ISO times. */
	private Pattern _isoTimePattern;

	/**
     * Creates a new model reader for Microsoft Sql Server databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public MSSqlModelReader(Platform platform)
    {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");

    	try
    	{
            _isoDatePattern = Pattern.compile("'(\\d{4}\\-\\d{2}\\-\\d{2})'");
            _isoTimePattern = Pattern.compile("'(\\d{2}:\\d{2}:\\d{2})'");
        }
    	catch (PatternSyntaxException ex)
        {
        	throw new DdlUtilsException(ex);
        }
    }


    /**
     * {@inheritDoc}
     */
	protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
	{
        String tableName = (String)values.get("TABLE_NAME");

        for (int idx = 0; idx < KNOWN_SYSTEM_TABLES.length; idx++)
        {
            if (KNOWN_SYSTEM_TABLES[idx].equals(tableName))
            {
                return null;
            }
        }

        Table table = super.readTable(metaData, values);

        if (table != null)
        {
            //todo 查询表注释信息
//            SELECT objname, cast(value as varchar(8000)) as value
//            FROM fn_listextendedproperty ('MS_DESCRIPTION','schema', 'dbo', 'table', 'bf_role_', 'column', null)
            table = readComments(table);

            // Sql Server does not return the auto-increment status via the database metadata
            determineAutoIncrementFromResultSetMetaData(table, table.getColumns());

            // TODO: Replace this manual filtering using named pks once they are available
            //       This is then probably of interest to every platform
            for (int idx = 0; idx < table.getIndexCount();)
            {
                Index index = table.getIndex(idx);

                if (index.isUnique() && existsPKWithName(metaData, table, index.getName()))
                {
                    table.removeIndex(idx);
                }
                else
                {
                    idx++;
                }
            }
        }
        return table;
	}

    /**
     * 查询表的注释信息
     * 由于DatabaseMetaData.getTables(..)获取不到remarks Mssql单独处理
     * @param table
     * @return
     */
    private Table readComments(Table table) {

//        String sql="SELECT objtype,objname,name,value FROM fn_listextendedproperty (NULL, 'schema', 'Sales', 'table', default, NULL, NULL)";
        String sql = "SELECT objtype,objname,name,cast(value as varchar(8000)) as value FROM fn_listextendedproperty (?, ?, ?, ?, ?, NULL, NULL)";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = getConnection().prepareStatement(sql);
            pstmt.setString(1, "MS_DESCRIPTION");
            pstmt.setString(2, "schema");
            pstmt.setString(3, "dbo");
            pstmt.setString(4, "table");
            pstmt.setString(5, table.getName());

            rs = pstmt.executeQuery();
            while (rs.next()) {
                table.setDescription(rs.getString("value"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(pstmt);
            closeResultSet(rs);
        }
        return table;
    }

    /**
     * {@inheritDoc}
     */
	protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table, Index index)
	{
		// Sql Server generates an index "PK__[table name]__[hex number]"
		StringBuffer pkIndexName = new StringBuffer();

		pkIndexName.append("PK__");
		pkIndexName.append(table.getName());
		pkIndexName.append("__");

		return index.getName().toUpperCase().startsWith(pkIndexName.toString().toUpperCase());
	}

    /**
     * Determines whether there is a pk for the table with the given name.
     * 
     * @param metaData The database metadata
     * @param table    The table
     * @param name     The pk name
     * @return <code>true</code> if there is such a pk
     */
    private boolean existsPKWithName(DatabaseMetaDataWrapper metaData, Table table, String name) throws SQLException
    {
        ResultSet pks = null;

        try
        {
            pks = metaData.getPrimaryKeys(metaData.escapeForSearch(table.getName()));

            while (pks.next())
            {
                if (name.equals(pks.getString("PK_NAME")))
                {
                    return true;
                }
            }
            return false;
        }
        finally
        {
            closeResultSet(pks);
        }
    }
    
    /**
     * {@inheritDoc}
     */
	protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
	{
		Column column       = super.readColumn(metaData, values);
		String defaultValue = column.getDefaultValue();

		// Sql Server tends to surround the returned default value with one or two sets of parentheses
		if (defaultValue != null)
		{
			while (defaultValue.startsWith("(") && defaultValue.endsWith(")"))
			{
				defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
			}

			if (column.getTypeCode() == Types.TIMESTAMP)
			{
				// Sql Server maintains the default values for DATE/TIME jdbc types, so we have to
				// migrate the default value to TIMESTAMP
				Matcher   matcher   = _isoDatePattern.matcher(defaultValue);
				Timestamp timestamp = null;
	
				if (matcher.matches())
				{
					timestamp = new Timestamp(Date.valueOf(matcher.group(1)).getTime());
				}
				else
				{
	                matcher = _isoTimePattern.matcher(defaultValue);

	                if (matcher.matches())
	                {
	                    timestamp = new Timestamp(Time.valueOf(matcher.group(1)).getTime());
	                }
				}
				if (timestamp != null)
				{
					defaultValue = timestamp.toString();
				}
			}
			else if (column.getTypeCode() == Types.DECIMAL)
			{
				// For some reason, Sql Server 2005 always returns DECIMAL default values with a dot
				// even if the scale is 0, so we remove the dot
				if ((column.getScale() == 0) && defaultValue.endsWith("."))
				{
					defaultValue = defaultValue.substring(0, defaultValue.length() - 1);
				}
			}
            else if (TypeMap.isTextType(column.getTypeCode()))
            {
                defaultValue = unescape(defaultValue, "'", "''");
            }
            
			column.setDefaultValue(defaultValue);
		}
		if ((column.getTypeCode() == Types.DECIMAL) && (column.getSizeAsInt() == 19) && (column.getScale() == 0))
		{
			column.setTypeCode(Types.BIGINT);
		}

		return column;
	}

    /**
     * Reads the column definitions for the indicated table.
     *
     * @param metaData  The database meta data
     * @param tableName The name of the table
     * @return The columns
     */
    protected Collection readColumns(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        ResultSet columnData = null;

        try
        {
            columnData = metaData.getColumns(metaData.escapeForSearch(tableName), getDefaultColumnPattern());

            List<Column> columns = new ArrayList<Column>();


            while (columnData.next())
            {
                Map values = readColumns(columnData, getColumnsForColumn());

                columns.add(readColumn(metaData, values));
            }

            columns = readColumnsCommnets(columns,tableName);

            return columns;
        }
        finally
        {
            closeResultSet(columnData);
        }
    }

    /**
     * 查询字段注释
     *
     * @param columns
     * @param tableName
     * @return
     */
    private List<Column> readColumnsCommnets(List<Column> columns, String tableName) throws SQLException {
        //todo 查询字段注释
//            SELECT objname, cast(value as varchar(8000)) as value
//            FROM fn_listextendedproperty ('MS_DESCRIPTION','schema', 'dbo', 'table', 'bf_role_', 'column', null)
        String sql = "SELECT objtype,objname,name,cast(value as varchar(8000)) as value FROM fn_listextendedproperty (?, ?, ?, ?, ?, 'column', NULL)";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = getConnection().prepareStatement(sql);
            pstmt.setString(1, "MS_DESCRIPTION");
            pstmt.setString(2, "schema");
            pstmt.setString(3, "dbo");
            pstmt.setString(4, "table");
            pstmt.setString(5, tableName);

            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tempColumn = rs.getString("objname");
                for (Column column : columns) {
                    if (column.getName().equalsIgnoreCase(tempColumn)) {
                        column.setDescription(rs.getString("value"));
                    }
                }
            }
        } finally {
            closeStatement(pstmt);
            closeResultSet(rs);
        }
        return columns;
    }
}
