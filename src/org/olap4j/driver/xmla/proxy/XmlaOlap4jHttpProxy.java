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

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.olap4j.driver.xmla.XmlaOlap4jDriver;
import org.olap4j.driver.xmla.XmlaOlap4jServerInfos;
import org.olap4j.impl.Base64;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

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
    private final static HttpClient httpClient = new HttpClient();

    /**
     * Creates a XmlaOlap4jHttpProxy.
     *
     * @param driver Driver
     */
    public XmlaOlap4jHttpProxy(
        XmlaOlap4jDriver driver)
    {
        this.driver = driver;
    }

    private static final String DISCOVER =
        "<Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"";

    private static final String EXECUTE =
        "<Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\"";

    @Override
    public ByteBuffer getResponse(XmlaOlap4jServerInfos serverInfos, final String request)
        throws XmlaOlap4jProxyException
    {
        URLConnection urlConnection = null;
        try {
            if(!httpClient.isStarted() || httpClient.isStarting())
                httpClient.start();
            URL url = serverInfos.getUrl();
            // Open connection to manipulate the properties
//            urlConnection = url.openConnection();
//            urlConnection.setDoOutput(true);

            // Set headers
            Request req = httpClient.newRequest(url.toURI());
            req.getHeaders().clear();
            req.method(HttpMethod.POST);
            req.header(
                "content-type",
                "text/xml; charset="
                    .concat(getEncodingCharsetName()));
            req.header(
                "User-Agent",
                "Olap4j("
                    .concat(driver.getVersion())
                    .concat(")"));
            req.header(
                "Accept",
                "text/xml;q=1");
            req.header(
                "Accept-Charset",
                getEncodingCharsetName()
                    .concat(";q=1"));

            // Tell the server that we support gzip encoding
            req.header(
                "Accept-Encoding",
                "gzip");

            // Some servers expect a SOAPAction header.
            // TODO There is bound to be a better way to do this.
            if (request.contains(DISCOVER)) {
                req.header(
                    "SOAPAction",
                    "\"urn:schemas-microsoft-com:xml-analysis:Discover\"");
            } else if (request.contains(EXECUTE)) {
                req.header(
                    "SOAPAction",
                    "\"urn:schemas-microsoft-com:xml-analysis:Execute\"");
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
                req.header(
                    "Authorization", "Basic " + encoding);
            }

            // Set correct cookies
            this.useCookies(req);

            // Send data (i.e. POST). Use same encoding as specified in the
            // header.
            final String encoding = getEncodingCharsetName();
            req.content(new BytesContentProvider("text/xml; charset="
                    .concat(getEncodingCharsetName()), request.getBytes(encoding)));


            // Get the response, again assuming default encoding.
//            InputStream is = urlConnection.getInputStream();

            // Detect that the server used gzip encoding
//            String contentEncoding =
//                urlConnection.getHeaderField("Content-Encoding");
//            if ("gzip".equals(contentEncoding)) {
//                is = new GZIPInputStream(is);
//            }

            Path tempFile = Files.createTempFile("olap4j", "result");
            ByteBuffer tmpByteBuffer;
            try (FileChannel tmpFileChannel = FileChannel.open(tempFile, StandardOpenOption.DELETE_ON_CLOSE, StandardOpenOption.READ, StandardOpenOption.WRITE)){
                CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
                req.send(new Response.Listener.Adapter()
                {
                    @Override
                    public void onContent(Response response, ByteBuffer buffer)
                    {
                        try {
                            tmpFileChannel.write(buffer);
                        } catch (IOException e) {
                            response.abort(e);
                            future.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void onFailure(Response response, Throwable failure) {
                        response.abort(failure);
                        future.completeExceptionally(failure);
                    }

                    @Override
                    public void onComplete(Result result) {
                        MappedByteBuffer map;
                        try {
                            map = tmpFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, tmpFileChannel.size());
                        } catch (IOException e) {
                            future.completeExceptionally(e);
                            return;
                        }
                        if(result.getResponse().getStatus() >= 400) {
                            future.completeExceptionally(new XmlaOlap4jProxyException("\n" + StandardCharsets.UTF_8.decode(map).toString(), null));
                        } else {
                            future.complete(map);
                        }
                    }

                    @Override
                    public void onHeaders(Response response) {
                        XmlaOlap4jHttpProxy.this.saveCookies(response);
                    }
                });
//                byte[] buf = new byte[8192];
//                int count;

//                while ((count = is.read(buf)) > 0) {
//                    tmpFileChannel.write(ByteBuffer.wrap(buf, 0, count));
//                }

                tmpByteBuffer = future.get();
            }

            // Save the returned cookies for later use
//            this.saveCookies(req);

            return tmpByteBuffer;
        // All exceptions should be trapped here.
        // The response will only be available here anyways.
        } catch (Exception e) {
            // In order to prevent the JDK from keeping this connection
            // in WAIT mode, we need to empty the error stream cache.
//            try {
//                final int espCode =
//                    ((HttpURLConnection)urlConnection).getResponseCode();
//                InputStream errorStream =
//                    ((HttpURLConnection)urlConnection).getErrorStream();
//                final ByteArrayOutputStream baos =
//                    new ByteArrayOutputStream();
//                final byte[] buf = new byte[1024];
//                int count;
//                if (errorStream != null) {
//                    while ((count = errorStream.read(buf)) > 0) {
//                        baos.write(buf, 0, count);
//                    }
//                    errorStream.close();
//                }
//                baos.close();
//            } catch (IOException ex) {
//                // Well, we tried. No point notifying the user here.
//            }
            throw new XmlaOlap4jProxyException(
                "This proxy encountered an exception while processing the "
                + "query.",
                e);
        }
    }

    @Override
    public Future<ByteBuffer> getResponseViaSubmit(
        final XmlaOlap4jServerInfos serverInfos,
        final String request)
    {
        return XmlaOlap4jDriver.getFuture(this, serverInfos, request);
    }

    // implement XmlaOlap4jProxy
    public String getEncodingCharsetName() {
        return "UTF-8";
    }
}

// End XmlaOlap4jHttpProxy.java



