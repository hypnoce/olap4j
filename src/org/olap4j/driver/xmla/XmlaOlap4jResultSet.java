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

import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.MDDATASET_NS;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.SOAP_NS;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.XMLA_NS;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.findChild;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.parse;

abstract class XmlaOlap4jResultSet implements ResultSet {
    private static final String VALUE_TAG = "Value";
    private final static DateTimeFormatter LENIENT_ISO_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .parseLenient()
    .append(ISO_LOCAL_DATE)
    .appendLiteral('T')
    .append(ISO_LOCAL_TIME)
    .toFormatter();

    private static final boolean DEBUG = false;

    enum XsdTypes {
        XSD_INT("xsd:int", JDBCType.INTEGER),
        XSD_INTEGER("xsd:integer", JDBCType.INTEGER),
        XSD_DOUBLE("xsd:double", JDBCType.DOUBLE),
        XSD_POSITIVEINTEGER("xsd:positiveInteger", JDBCType.INTEGER),
        XSD_DECIMAL("xsd:decimal", JDBCType.DECIMAL),
        XSD_SHORT("xsd:short", JDBCType.SMALLINT),
        XSD_FLOAT("xsd:float", JDBCType.FLOAT),
        XSD_LONG("xsd:long", JDBCType.BIGINT),
        XSD_BOOLEAN("xsd:boolean", JDBCType.BOOLEAN),
        XSD_BYTE("xsd:byte", JDBCType.TINYINT),
        XSD_UNSIGNEDBYTE("xsd:unsignedByte", JDBCType.TINYINT),
        XSD_UNSIGNEDSHORT("xsd:unsignedShort", JDBCType.SMALLINT),
        XSD_UNSIGNEDLONG("xsd:unsignedLong", JDBCType.BIGINT),
        XSD_UNSIGNEDINT("xsd:unsignedInt", JDBCType.INTEGER),
        XSD_STRING("xsd:string", JDBCType.VARCHAR),
        XSD_DATETIME("xsd:dateTime", JDBCType.DATE);

        private final String name;
        private final JDBCType sqlType;
        private static final Map<String, XsdTypes> TYPES;
        private static final Map<SignedType, XsdTypes> SQL_TYPES;
        static {
            Map<String, XsdTypes> typesInitial = new HashMap<>();
            Map<SignedType, XsdTypes> sqlTypesInitial = new HashMap<>();
            for (XsdTypes type : values()) {
                typesInitial.put(type.name, type);
                sqlTypesInitial.put(new SignedType(type.getSqlType().getVendorTypeNumber(), !type.name.contains("unsigned")), type);
            }
            TYPES = Collections.unmodifiableMap(typesInitial);
            SQL_TYPES = Collections.unmodifiableMap(sqlTypesInitial);
        }

        XsdTypes(String name, JDBCType sqlType) {
            this.name = name;
            this.sqlType = sqlType;
        }

        public static XsdTypes fromString(String name) {
            XsdTypes type = TYPES.get(name);
            return type == null ? XSD_STRING : type;
        }

        public static XsdTypes fromSQLType(int sqlType, boolean signed) {
            XsdTypes type = SQL_TYPES.get(new SignedType(sqlType, signed));
            return type == null ? XSD_STRING : type;
        }

        public JDBCType getSqlType() {
            return sqlType;
        }
    }

    private static class SignedType {
       private final int sqlVendorType;
       private final boolean signed;

       private SignedType(int sqlVendorType, boolean signed) {
          this.sqlVendorType = sqlVendorType;
          this.signed = signed;
       }

       @Override
       public boolean equals(Object o) {
          if (this == o) {
             return true;
          }
          if (o == null || getClass() != o.getClass()) {
             return false;
          }
          SignedType that = (SignedType) o;
          return sqlVendorType == that.sqlVendorType &&
            signed == that.signed;
       }

       @Override
       public int hashCode() {
          return Objects.hash(sqlVendorType, signed);
       }
    }

    final XmlaOlap4jStatement olap4jStatement;
    protected boolean closed;

    protected XmlaOlap4jResultSet(XmlaOlap4jStatement olap4jStatement) {
        Objects.requireNonNull(olap4jStatement);
        this.olap4jStatement = olap4jStatement;
        this.closed = false;
    }

    /**
     * Returns the error-handler.
     *
     * @return Error handler
     */
    protected XmlaHelper getHelper() {
        return olap4jStatement.olap4jConnection.helper;
    }

    /**
     * Gets response from the XMLA request and populates cell set axes and cells
     * with it.
     *
     * @throws OlapException on error
     */
    void populate() throws OlapException {
        byte[] bytes = olap4jStatement.getBytes();

        Document doc;
        try {
            doc = parse(bytes);
        } catch (IOException e) {
            throw getHelper().createException(
                    "error creating CellSet", e);
        } catch (SAXException e) {
            throw getHelper().createException(
                    "error creating CellSet", e);
        }
        // <SOAP-ENV:Envelope>
        //   <SOAP-ENV:Header/>
        //   <SOAP-ENV:Body>
        //     <xmla:ExecuteResponse>
        //       <xmla:return>
        //         <root>
        //           (see below)
        //         </root>
        //       <xmla:return>
        //     </xmla:ExecuteResponse>
        //   </SOAP-ENV:Body>
        // </SOAP-ENV:Envelope>
        final Element envelope = doc.getDocumentElement();
        if (DEBUG) {
            System.out.println(XmlaOlap4jUtil.toString(doc, true));
        }
        assert envelope.getLocalName().equals("Envelope");
        assert envelope.getNamespaceURI().equals(SOAP_NS);
        Element body =
                findChild(envelope, SOAP_NS, "Body");
        Element fault =
                findChild(body, SOAP_NS, "Fault");
        if (fault != null) {
            // <SOAP-ENV:Fault>
            //     <faultcode>SOAP-ENV:Client.00HSBC01</faultcode>
            //     <faultstring>XMLA connection datasource not
            //                  found</faultstring>
            //     <faultactor>Mondrian</faultactor>
            //     <detail>
            //         <XA:error xmlns:XA="http://mondrian.sourceforge.net">
            //             <code>00HSBC01</code>
            //             <desc>The Mondrian XML: Mondrian Error:Internal
            //                 error: no catalog named 'LOCALDB'</desc>
            //         </XA:error>
            //     </detail>
            // </SOAP-ENV:Fault>
            //
            // TODO: log doc to logfile
            throw getHelper().createException(
                    "XMLA provider gave exception: "
                            + XmlaOlap4jUtil.prettyPrint(fault));
        }
        Element executeResponse =
                findChild(body, XMLA_NS, "ExecuteResponse");
        Element returnElement =
                findChild(executeResponse, XMLA_NS, "return");
        parseReturnElement(returnElement);
    }

    protected abstract void parseReturnElement(Element returnElement) throws OlapException;

    /**
     * Returns the value of a cell, cast to the appropriate Java object type
     * corresponding to the XML schema (XSD) type of the value.
     *
     * <p>The value type must conform to XSD definitions of the XML element. See
     * <a href="http://books.xmlschemata.org/relaxng/relax-CHP-19.html">RELAX
     * NG, Chapter 19</a> for a full list of possible data types.
     *
     * <p>This method does not currently support all types; most numeric types
     * are supported, but no dates are yet supported. Those not supported
     * fall back to Strings.
     *
     * @param cell The cell of which we want the casted object.
     * @return The object with a correct value.
     * @throws OlapException if any error is encountered while casting the cell
     * value
     */
    protected Object getTypedValue(Element cell) throws OlapException {
        Element elm = findChild(cell, MDDATASET_NS, "Value");
        if (elm == null) {
            // Cell is null.
            return null;
        }

        // The object type is contained in xsi:type attribute.
        String type = elm.getAttribute("xsi:type");
        XsdTypes xsdType =  XsdTypes.fromString(elm.getAttribute("xsi:type"));
        return getTypedValue(cell, VALUE_TAG, xsdType);
    }

    /**
     * Returns the value of a cell, cast to the appropriate Java object type
     * corresponding to the XML schema (XSD) type of the value.
     *
     * <p>The value type must conform to XSD definitions of the XML element. See
     * <a href="http://books.xmlschemata.org/relaxng/relax-CHP-19.html">RELAX
     * NG, Chapter 19</a> for a full list of possible data types.
     *
     * <p>This method does not currently support all types; most numeric types
     * are supported, but no dates are yet supported. Those not supported
     * fall back to Strings.
     *
     * @param row The row of which we want the casted object.
     * @return The object with a correct value.
     * @throws OlapException if any error is encountered while casting the cell
     * value
     */
    protected Object getTypedValue(Element row, String name, XsdTypes xsdType) throws OlapException {
        try {
            switch (xsdType) {
                case XSD_BOOLEAN:
                    return XmlaOlap4jUtil.booleanElement(row, name);
                case XSD_INT:
                    return XmlaOlap4jUtil.intElement(row, name);
                case XSD_INTEGER:
                    return XmlaOlap4jUtil.bigIntegerElement(row, name);
                case XSD_DOUBLE:
                    return XmlaOlap4jUtil.doubleElement(row, name);
                case XSD_POSITIVEINTEGER:
                    return XmlaOlap4jUtil.bigIntegerElement(row, name);
                case XSD_DECIMAL:
                    return XmlaOlap4jUtil.bigDecimalElement(row, name);
                case XSD_SHORT:
                    return XmlaOlap4jUtil.shortElement(row, name);
                case XSD_FLOAT:
                    return XmlaOlap4jUtil.floatElement(row, name);
                case XSD_LONG:
                    return XmlaOlap4jUtil.longElement(row, name);
                case XSD_BYTE:
                    return XmlaOlap4jUtil.byteElement(row, name);
                case XSD_UNSIGNEDBYTE:
                    return XmlaOlap4jUtil.shortElement(row, name);
                case XSD_UNSIGNEDSHORT:
                    return XmlaOlap4jUtil.intElement(row, name);
                case XSD_UNSIGNEDLONG:
                    return XmlaOlap4jUtil.bigDecimalElement(row, name);
                case XSD_UNSIGNEDINT:
                    return XmlaOlap4jUtil.longElement(row, name);
                case XSD_DATETIME:
                    String text = XmlaOlap4jUtil.stringElement(row, name);
                    if(text == null) return null;
                    return Date.from(LocalDateTime.parse(text, LENIENT_ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC));
                default:
                    return XmlaOlap4jUtil.stringElement(row, name);
            }
        } catch (Exception e) {
            throw getHelper().createException(
                    "Error while casting a cell value to the correct java type for"
                            + " its XSD type " + xsdType,
                    e);
        }
    }

    public boolean next() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void close() throws SQLException {
        this.closed = true;
    }

    public boolean wasNull() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null : Objects.toString(object);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return (boolean)getObject(columnIndex);
    }

    public byte getByte(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        return o == null ? 0 : ((Number)o).byteValue();
    }

    public short getShort(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        return o == null ? 0 : ((Number)o).shortValue();
    }

    public int getInt(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        return o == null ? 0 : ((Number)o).intValue();
    }

    public long getLong(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        return o == null ? 0 : ((Number)o).longValue();
    }

    public float getFloat(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        return o == null ? 0 : ((Number)o).floatValue();
    }

    public double getDouble(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        return o == null ? 0 : ((Number)o).doubleValue();
    }

    public BigDecimal getBigDecimal(
            int columnIndex, int scale) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    public Time getTime(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(
            String columnLabel, int scale) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    public Time getTime(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    public int findColumn(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(
            int columnIndex, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(int columnIndex, byte x[]) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(
            int columnIndex, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
            int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
            int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
            int columnIndex, Reader x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(
            int columnIndex, Object x, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(
            String columnLabel, boolean x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(
            String columnLabel, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(String columnLabel, byte x[]) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(
            String columnLabel, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
            String columnLabel, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
            String columnLabel, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
            String columnLabel, Reader reader, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(
            String columnLabel, Object x, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public OlapStatement getStatement() {
        return olap4jStatement;
    }

    public Object getObject(
            int columnIndex, Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(
            String columnLabel, Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
            int columnIndex, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
            String columnLabel, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
