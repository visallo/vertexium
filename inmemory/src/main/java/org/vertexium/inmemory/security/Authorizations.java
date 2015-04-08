/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.vertexium.inmemory.security;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * A collection of authorization strings.
 */
public class Authorizations implements Iterable<byte[]>, Serializable, AuthorizationContainer {

    private static final long serialVersionUID = 1L;

    private Set<ByteSequence> auths = new HashSet<ByteSequence>();
    private List<byte[]> authsList = new ArrayList<byte[]>(); // sorted order

    /**
     * An empty set of authorizations.
     */
    public static final Authorizations EMPTY = new Authorizations();

    private static final boolean[] validAuthChars = new boolean[256];

    static {
        for (int i = 0; i < 256; i++) {
            validAuthChars[i] = false;
        }

        for (int i = 'a'; i <= 'z'; i++) {
            validAuthChars[i] = true;
        }

        for (int i = 'A'; i <= 'Z'; i++) {
            validAuthChars[i] = true;
        }

        for (int i = '0'; i <= '9'; i++) {
            validAuthChars[i] = true;
        }

        validAuthChars['_'] = true;
        validAuthChars['-'] = true;
        validAuthChars[':'] = true;
        validAuthChars['.'] = true;
        validAuthChars['/'] = true;
    }

    static final boolean isValidAuthChar(byte b) {
        return validAuthChars[0xff & b];
    }

    private void checkAuths() {
        Set<ByteSequence> sortedAuths = new TreeSet<ByteSequence>(auths);

        for (ByteSequence bs : sortedAuths) {
            if (bs.length() == 0) {
                throw new IllegalArgumentException("Empty authorization");
            }

            authsList.add(bs.toArray());
        }
    }

    /**
     * Constructs an authorization object from a collection of string authorizations that have each already been encoded as UTF-8 bytes. Warning: This method does
     * not verify that each encoded string is valid UTF-8.
     *
     * @param authorizations collection of authorizations, as strings encoded in UTF-8
     * @throws IllegalArgumentException if authorizations is null
     * @see #Authorizations(String...)
     */
    public Authorizations(Collection<byte[]> authorizations) {
        for (byte[] auth : authorizations)
            auths.add(new ArrayByteSequence(auth));
        checkAuths();
    }

    /**
     * Constructs an authorization object from a list of string authorizations that have each already been encoded as UTF-8 bytes. Warning: This method does not
     * verify that each encoded string is valid UTF-8.
     *
     * @param authorizations list of authorizations, as strings encoded in UTF-8 and placed in buffers
     * @throws IllegalArgumentException if authorizations is null
     * @see #Authorizations(String...)
     */
    public Authorizations(List<ByteBuffer> authorizations) {
        for (ByteBuffer buffer : authorizations) {
            auths.add(new ArrayByteSequence(ByteBufferUtil.toBytes(buffer)));
        }
        checkAuths();
    }

    /**
     * Constructs an empty set of authorizations.
     *
     * @see #Authorizations(String...)
     */
    public Authorizations() {
    }

    /**
     * Constructs an authorizations object from a set of human-readable authorizations.
     *
     * @param authorizations array of authorizations
     * @throws IllegalArgumentException if authorizations is null
     */
    public Authorizations(String... authorizations) {
        setAuthorizations(authorizations);
    }

    private void setAuthorizations(String... authorizations) {
        auths.clear();
        for (String str : authorizations) {
            str = str.trim();
            auths.add(new ArrayByteSequence(str.getBytes(Constants.UTF8)));
        }

        checkAuths();
    }

    /**
     * Gets the authorizations in sorted order. The returned list is not modifiable.
     *
     * @return authorizations, each as a string encoded in UTF-8
     * @see #Authorizations(Collection)
     */
    public List<byte[]> getAuthorizations() {
        ArrayList<byte[]> copy = new ArrayList<byte[]>(authsList.size());
        for (byte[] auth : authsList) {
            byte[] bytes = new byte[auth.length];
            System.arraycopy(auth, 0, bytes, 0, auth.length);
            copy.add(bytes);
        }
        return Collections.unmodifiableList(copy);
    }

    /**
     * Gets the authorizations in sorted order. The returned list is not modifiable.
     *
     * @return authorizations, each as a string encoded in UTF-8 and within a buffer
     */
    public List<ByteBuffer> getAuthorizationsBB() {
        ArrayList<ByteBuffer> copy = new ArrayList<ByteBuffer>(authsList.size());
        for (byte[] auth : authsList) {
            byte[] bytes = new byte[auth.length];
            System.arraycopy(auth, 0, bytes, 0, auth.length);
            copy.add(ByteBuffer.wrap(bytes));
        }
        return Collections.unmodifiableList(copy);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (ByteSequence auth : auths) {
            sb.append(sep);
            sep = ",";
            sb.append(new String(auth.toArray(), Constants.UTF8));
        }

        return sb.toString();
    }

    /**
     * Checks whether this object contains the given authorization.
     *
     * @param auth authorization, as a string encoded in UTF-8
     * @return true if authorization is in this collection
     */
    public boolean contains(byte[] auth) {
        return auths.contains(new ArrayByteSequence(auth));
    }

    /**
     * Checks whether this object contains the given authorization. Warning: This method does not verify that the encoded string is valid UTF-8.
     *
     * @param auth authorization, as a string encoded in UTF-8
     * @return true if authorization is in this collection
     */
    @Override
    public boolean contains(ByteSequence auth) {
        return auths.contains(auth);
    }

    /**
     * Checks whether this object contains the given authorization.
     *
     * @param auth authorization
     * @return true if authorization is in this collection
     */
    public boolean contains(String auth) {
        return auths.contains(new ArrayByteSequence(auth));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (o instanceof Authorizations) {
            Authorizations ao = (Authorizations) o;

            return auths.equals(ao.auths);
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (ByteSequence b : auths)
            result += b.hashCode();
        return result;
    }

    /**
     * Gets the size of this collection of authorizations.
     *
     * @return collection size
     */
    public int size() {
        return auths.size();
    }

    /**
     * Checks if this collection of authorizations is empty.
     *
     * @return true if this collection contains no authorizations
     */
    public boolean isEmpty() {
        return auths.isEmpty();
    }

    @Override
    public Iterator<byte[]> iterator() {
        return getAuthorizations().iterator();
    }
}
