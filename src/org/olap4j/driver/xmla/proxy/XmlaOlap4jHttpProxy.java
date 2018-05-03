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
package org.olap4j.driver.xmla.proxy;

import java.util.Map;
import org.olap4j.driver.xmla.XmlaOlap4jDriver;
import org.olap4j.driver.xmla.XmlaOlap4jServerInfos;
import org.olap4j.impl.Base64;

import java.io.*;
import java.net.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Extends the AbstractCachedProxy and serves as
 * a production ready http communication class. Every SOAP request
 * sends a POST call to the destination XMLA server and returns
 * the response as a byte array, conforming to the Proxy interface.
 *
 * <p>It also takes advantage of the AbstractHttpProxy cookie
 * managing facilities. All cookies received from the end point
 * server will be sent back if they are not expired and they also
 * conform to cookie domain rules.
 *
 * @author Luc Boudreau and Julian Hyde
 */
public class XmlaOlap4jHttpProxy extends XmlaOlap4jAbstractHttpProxy
{
    private final XmlaOlap4jDriver driver;
    private final Client client;

    /**
     * Creates a XmlaOlap4jHttpProxy.
     *
     * @param driver Driver
     */
    public XmlaOlap4jHttpProxy(
        XmlaOlap4jDriver driver, Map<String, String> properties)
    {
        this.driver = driver;
        String tracingServiceName = properties.get(XmlaOlap4jDriver.Property.ZIPKINSERVICENAME.name());
        if(null == tracingServiceName || "".equals(tracingServiceName)) {
            tracingServiceName = properties.get(XmlaOlap4jDriver.Property.CATALOG.name());
        }
        final ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        if(null != tracingServiceName && !"".equals(tracingServiceName)) {
            clientBuilder.property("TRACING_SERVICE_NAME", tracingServiceName);
        }
        this.client = clientBuilder.build();
    }

    private static final String DISCOVER =
        "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"";

    private static final String EXECUTE =
        "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\"";

    @Override
    public byte[] getResponse(XmlaOlap4jServerInfos serverInfos, String request)
        throws XmlaOlap4jProxyException
    {
        try {
           return submit(serverInfos, request).get();
        } catch (Exception e) {
            throw new XmlaOlap4jProxyException(
                "This proxy encountered an exception while processing the "
                + "query.",
                e);
        }
    }

    @Override
    public Future<byte[]> getResponseViaSubmit (
        final XmlaOlap4jServerInfos serverInfos,
        final String request) throws XmlaOlap4jProxyException {
       try {
          URL url = serverInfos.getUrl();
          final WebTarget target = client.target(serverInfos.getUrl().toURI());
          final Invocation.Builder invocation = target.request()
//            .header(HttpHeaders.ACCEPT_ENCODING,
//              "gzip")
            .header(HttpHeaders.ACCEPT_CHARSET, getEncodingCharsetName()
              .concat(";q=1"))
            .header(HttpHeaders.ACCEPT, MediaType.TEXT_XML + ";q=1");

          // Tell the server that we support gzip encoding
          // Some servers expect a SOAPAction header.
          // TODO There is bound to be a better way to do this.
          if (request.contains(DISCOVER)) {
             invocation.header("SOAPAction",
               "urn:schemas-microsoft-com:xml-analysis:Discover");
          } else if (request.contains(EXECUTE)) {
             invocation.header("SOAPAction",
               "urn:schemas-microsoft-com:xml-analysis:Execute");
          }

          // Encode credentials for basic authentication
          StringBuilder sb = new StringBuilder();
          if (serverInfos.getUsername() != null
            && serverInfos.getPassword() != null)
          {
             sb.append(serverInfos.getUsername());
             sb.append(":");
             sb.append(serverInfos.getPassword());
          } else if (url.getUserInfo() != null) {
             sb.append(url.getUserInfo());
          }
          if (!sb.toString().equals("")) {
             String encoding =
               Base64.encodeBytes(
                 sb.toString().getBytes(), 0);
             invocation.header("Authorization", "Basic " + encoding);
          }


          // Send data (i.e. POST). Use same encoding as specified in the
          // header.
          final String encoding = getEncodingCharsetName();

          // Get the response, again assuming default encoding.
          final CompletionStage<Response> rxResp = invocation.rx().post(Entity.entity(request.getBytes(encoding), MediaType.TEXT_XML + "; charset=" + getEncodingCharsetName()));
          final CompletableFuture<Response> future = rxResp.toCompletableFuture();
          return future.thenApply(resp -> {
             try {
                InputStream is = resp.readEntity(InputStream.class);
                // Detect that the server used gzip encoding
                if ("gzip".equals(resp.getHeaderString(HttpHeaders.CONTENT_ENCODING))) {
                   is = new GZIPInputStream(is);
                }
                //
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buf = new byte[1024];
                int count;

                while ((count = is.read(buf)) > 0) {
                   baos.write(buf, 0, count);
                }

                if(resp.getStatus() >= 400) {
                   throw new WebApplicationException(new String(baos.toByteArray()), resp.getStatus());
                }

                return baos.toByteArray();
             } catch (IOException e) {
                throw new RuntimeException(e);
             }
          });

          // All exceptions should be trapped here.
          // The response will only be available here anyways.
       } catch (Exception e) {
          throw new XmlaOlap4jProxyException(
            "This proxy encountered an exception while processing the "
              + "query.",
            e);

       }
//        return XmlaOlap4jDriver.getFuture(this, serverInfos, request);
    }

    // implement XmlaOlap4jProxy
    public String getEncodingCharsetName() {
        return "UTF-8";
    }

    @Override
    public void close() throws IOException {
        if(this.client != null) {
            this.client.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if(this.client != null) {
            this.client.close();
        }
        super.finalize();
    }
}

// End XmlaOlap4jHttpProxy.java



