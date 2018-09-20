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

import static org.olap4j.driver.xmla.XmlaOlap4jUtil.childElements;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.findChild;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import javax.sql.RowSet;
import javax.sql.RowSetListener;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.olap4j.OlapException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XmlaOlap4jRowSet extends XmlaOlap4jResultSet implements RowSet {
    private final static XPath XPATH = XPathFactory.newInstance().newXPath();
    private final List<Element> data = new ArrayList<>();
    private RowSetMetaData rowSetMetaData;
    private int cursor = -1;
    protected XmlaOlap4jRowSet(XmlaOlap4jStatement olap4jStatement) {
        super(olap4jStatement);
    }

    @Override
    public boolean next() throws SQLException {
        return ++cursor < data.size();
    }

    @Override
    public String getUrl() throws SQLException {
        return null;
    }

    @Override
    public void setUrl(String url) throws SQLException {

    }

    @Override
    public String getDataSourceName() {
        return null;
    }

    @Override
    public void setDataSourceName(String name) throws SQLException {

    }

    @Override
    public String getUsername() {
        return "";
    }

    @Override
    public void setUsername(String name) throws SQLException {

    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public void setPassword(String password) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() {
        return 0;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

   @Override
   public boolean isBeforeFirst() throws SQLException {
      return !data.isEmpty() && cursor == -1;
   }

   @Override
   public boolean isAfterLast() throws SQLException {
      return !data.isEmpty() && cursor == data.size();
   }

   @Override
   public boolean isFirst() throws SQLException {
      return !data.isEmpty() && cursor == 0;
   }

   @Override
   public boolean isLast() throws SQLException {
      return !data.isEmpty() && cursor == data.size() - 1;
   }

   @Override
   public void beforeFirst() throws SQLException {
      cursor = -1;
   }

   @Override
   public void afterLast() throws SQLException {
      if(!data.isEmpty())
         cursor = data.size();
   }

   @Override
   public boolean first() throws SQLException {
      if(data.isEmpty()) {
         return false;
      }
      cursor = 0;
      return true;
   }

   @Override
   public boolean last() throws SQLException {
      cursor = data.size() - 1;
      return cursor > -1 && cursor < data.size();
   }

   @Override
   public int getRow() throws SQLException {
      return cursor + 1;
   }

   @Override
   public boolean absolute(int row) throws SQLException {
      if(row < 0) {
         cursor = Math.max(data.size() + row, -1);
      } else {
         cursor = Math.max(row - 1, -1);
      }
      return cursor > -1 && cursor < data.size();
   }

   @Override
   public boolean relative(int rows) throws SQLException {
      cursor += rows;
      return cursor > -1 && cursor < data.size();
   }

   @Override
   public boolean previous() throws SQLException {
      cursor = Math.max(-1, cursor - 1);
      return cursor == -1;
   }

   @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public String getCommand() {
        return null;
    }

    @Override
    public void setCommand(String cmd) throws SQLException {

    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setReadOnly(boolean value) throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public boolean getEscapeProcessing() throws SQLException {
        return false;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void setType(int type) throws SQLException {

    }

    @Override
    public void setConcurrency(int concurrency) throws SQLException {

    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {

    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {

    }

    @Override
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {

    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {

    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {

    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {

    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {

    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {

    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {

    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {

    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {

    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {

    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {

    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {

    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {

    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {

    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {

    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {

    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {

    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {

    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int i, Blob x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {

    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setClob(int i, Clob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {

    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {

    }

    @Override
    public void setArray(int i, Array x) throws SQLException {

    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {

    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {

    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {

    }

    @Override
    public void execute() throws SQLException {

    }

    @Override
    public void addRowSetListener(RowSetListener listener) {

    }

    @Override
    public void removeRowSetListener(RowSetListener listener) {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {

    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {

    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    /**
     * Gets response from the XMLA request and populates cell set axes and cells
     * with it.
     *
     * @throws OlapException on error
     */
    @Override
    protected void parseReturnElement(Element returnElement) throws OlapException {
        // <root> has children
        //   <xsd:schema/>
        //   <OlapInfo>
        //     <CubeInfo>
        //       <Cube>
        //         <CubeName>FOO</CubeName>
        //       </Cube>
        //     </CubeInfo>
        //     <AxesInfo>
        //       <AxisInfo/> ...
        //     </AxesInfo>
        //   </OlapInfo>
        //   <Axes>
        //      <Axis>
        //        <Tuples>
        //      </Axis>
        //      ...
        //   </Axes>
        //   <CellData>
        //      <Cell/>
        //      ...
        //   </CellData>

        final Element root =
                findChild(returnElement, XmlaOlap4jUtil.ROWSET_NS, "root");

        rowSetMetaData = new RowSetMetaDataImpl();
        try {
            NodeList columns = (NodeList) XPATH.evaluate(".//*[name()='xsd:complexType'][@name='row']//*[name()='xsd:element']", root, XPathConstants.NODESET);
            rowSetMetaData.setColumnCount(columns.getLength());
            for(int i = 0 ; i < columns.getLength() ; ++i) {
                String field = ((Element) columns.item(i)).getAttribute("sql:field");
                String name = ((Element) columns.item(i)).getAttribute("name");
                String type = ((Element) columns.item(i)).getAttribute("type");
                rowSetMetaData.setColumnName(i + 1, name);
                rowSetMetaData.setColumnLabel(i + 1, field);
                XsdTypes xsdType =  XsdTypes.fromString(type);
                rowSetMetaData.setColumnType(i + 1, xsdType.getSqlType().getVendorTypeNumber());
                rowSetMetaData.setColumnTypeName(i + 1, xsdType.getSqlType().getName());
                rowSetMetaData.setSigned(i + 1, !xsdType.name().contains("unsigned"));
            }
        } catch (XPathExpressionException | SQLException e) {
            throw new OlapException(e);
        }
        for (Element o : childElements(root)) {
            if (o.getLocalName().equals("row")) {
                data.add(o);
            }
        }
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return rowSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getTypedValue(data.get(cursor), getMetaData().getColumnName(columnIndex), XsdTypes.fromSQLType(getMetaData().getColumnType(columnIndex), getMetaData().isSigned(columnIndex)));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Objects.requireNonNull(columnLabel);
        return IntStream.range(1, getMetaData().getColumnCount() + 1).filter(i -> {
            try {
                return columnLabel.equals(getMetaData().getColumnLabel(i));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).findAny().orElseThrow(() -> new SQLException("Invalid column label " + columnLabel));
    }
}
