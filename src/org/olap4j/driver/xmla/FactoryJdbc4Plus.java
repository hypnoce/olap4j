/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.olap4j.driver.xmla;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.driver.xmla.proxy.XmlaOlap4jProxy;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Abstract JDBC classes, for JDBC 4.0 and 4.1.
 *
 * @author jhyde
 */
class FactoryJdbc4Plus {
    private FactoryJdbc4Plus() {
    }

    static abstract class AbstractEmptyResultSet extends EmptyResultSet {
        /**
         * Creates an AbstractEmptyResultSet.
         *
         * @param olap4jConnection Connection
         * @param headerList Column names
         * @param rowList List of row values
         */
        AbstractEmptyResultSet(
            XmlaOlap4jConnection olap4jConnection,
            List<String> headerList,
            List<List<Object>> rowList)
        {
            super(olap4jConnection, headerList, rowList);
        }
    }

    static abstract class AbstractConnection
        extends XmlaOlap4jConnection
        implements OlapConnection
    {
        /**
         * Creates an AbstractConnection.
         *
         * @param factory Factory
         * @param driver Driver
         * @param proxy Proxy
         * @param url URL
         * @param info Extra properties
         * @throws SQLException on error
         */
        AbstractConnection(
            Factory factory,
            XmlaOlap4jDriver driver,
            XmlaOlap4jProxy proxy,
            String url,
            Properties info) throws SQLException
        {
            super(factory, driver, proxy, url, info);
        }

    }

    static abstract class AbstractCellSet extends XmlaOlap4jCellSet {
        /**
         * Creates an AbstractCellSet.
         *
         * @param olap4jStatement Statement
         * @throws OlapException on error
         */
        AbstractCellSet(
            XmlaOlap4jStatement olap4jStatement)
            throws OlapException
        {
            super(olap4jStatement);
        }

        // implement java.sql.CellSet methods
        // introduced in JDBC 4.0/JDK 1.6
    }

    static abstract class AbstractPreparedStatement
        extends XmlaOlap4jPreparedStatement
    {
        /**
         * Creates a AbstractPreparedStatement.
         *
         * @param olap4jConnection Connection
         * @param mdx MDX query text
         * @throws OlapException on error
         */
        AbstractPreparedStatement(
            XmlaOlap4jConnection olap4jConnection,
            String mdx) throws OlapException
        {
            super(olap4jConnection, mdx);
        }
    }

    static abstract class AbstractDatabaseMetaData
        extends XmlaOlap4jDatabaseMetaData
    {
        /**
         * Creates an AbstractDatabaseMetaData.
         *
         * @param olap4jConnection Connection
         */
        AbstractDatabaseMetaData(
            XmlaOlap4jConnection olap4jConnection)
        {
            super(olap4jConnection);
        }
    }
}

// End FactoryJdbc4Plus.java
