package org.vertexium.type;

import org.vertexium.VertexiumException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpV4Address implements Serializable, Comparable<IpV4Address> {
    private static final long serialVersionUID = 42L;
    private static final Pattern IP_REGEX = Pattern.compile("^([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})$");
    private final int[] octets;

    public IpV4Address(String ipAddress) {
        Matcher m = IP_REGEX.matcher(ipAddress);
        if (!m.matches()) {
            throw new VertexiumException("Could not parse IP address: " + ipAddress);
        }
        octets = new int[4];
        for (int i = 0; i < 4; i++) {
            octets[i] = Integer.parseInt(m.group(i + 1));
        }
    }

    public IpV4Address(int a, int b, int c, int d) {
        this(new int[]{a, b, c, d});
    }

    public IpV4Address(int[] octets) {
        if (octets.length != 4) {
            throw new VertexiumException("Invalid IP address. Expected 4 octets, found " + octets.length);
        }
        this.octets = Arrays.copyOf(octets, 4);
    }

    public IpV4Address(byte[] octets) {
        if (octets.length != 4) {
            throw new VertexiumException("Invalid IP address. Expected 4 octets, found " + octets.length);
        }
        this.octets = new int[4];
        for (int i = 0; i < 4; i++) {
            this.octets[i] = octets[i];
        }
    }

    @Override
    public String toString() {
        return octets[0] + "." + octets[1] + "." + octets[2] + "." + octets[3];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IpV4Address ipAddress = (IpV4Address) o;

        if (!Arrays.equals(octets, ipAddress.octets)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return octets != null ? Arrays.hashCode(octets) : 0;
    }

    @Override
    public int compareTo(IpV4Address o) {
        for (int i = 0; i < 4; i++) {
            int eq = Integer.compare(this.octets[i], o.octets[i]);
            if (eq != 0) {
                return eq;
            }
        }
        return 0;
    }
}
