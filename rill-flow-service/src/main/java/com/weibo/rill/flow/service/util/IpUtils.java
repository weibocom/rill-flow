/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.weibo.rill.flow.service.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.regex.Pattern;

public class IpUtils {
    private IpUtils() {
        // empty
    }

    private static final String IP_REGEX = "((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}" +
            "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    private static final Pattern IPV4_PATTERN;
    private static final long[][] INTRANET_IP_RANGES;

    static {
        IPV4_PATTERN = Pattern.compile(IP_REGEX);
        INTRANET_IP_RANGES = new long[][]{{(long) ipv4ToInt("10.0.0.0"), (long) ipv4ToInt("10.255.255.255")},
                {(long) ipv4ToInt("172.16.0.0"), (long) ipv4ToInt("172.31.255.255")},
                {(long) ipv4ToInt("192.168.0.0"), (long) ipv4ToInt("192.168.255.255")}};
    }


    public static boolean isIpv4Address(String in) {
        return in != null && IPV4_PATTERN.matcher(in).matches();
    }

    public static int ipv4ToInt(String addr, boolean isSegment) {
        String[] addressBytes = addr.split("\\.");
        int length = addressBytes.length;
        if (length < 3) {
            return 0;
        } else {
            int ip = 0;

            try {
                for (int e = 0; e < 3; ++e) {
                    ip <<= 8;
                    ip |= Integer.parseInt(addressBytes[e]);
                }

                ip <<= 8;
                if (!isSegment && length != 3) {
                    ip |= Integer.parseInt(addressBytes[3]);
                }
            } catch (Exception var6) {
                // ignore exception
                return ip;
            }
            return ip;
        }
    }

    public static int ipv4ToInt(String addr) {
        return ipv4ToInt(addr, false);
    }

    public static String intToIpv4(int ipInInt) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(ipInInt);
        final byte[] array = buffer.array();
        try {
            final InetAddress byAddress = InetAddress.getByAddress(array);
            return byAddress.getHostAddress();
        } catch (UnknownHostException e) {
            // unreachable
            return null;
        }
    }

    public static boolean isIntranetIpv4Address(String ip) {
        if (!isIpv4Address(ip)) {
            return false;
        } else {
            long ipNum = ipv4ToInt(ip);

            for (long[] range : INTRANET_IP_RANGES) {
                if (ipNum >= range[0] && ipNum <= range[1]) {
                    return true;
                }
            }

            return false;
        }
    }

    public static long ipv4ToLong(String strIp) {
        long[] ip = new long[4];
        int position1 = strIp.indexOf('.');
        int position2 = strIp.indexOf('.', position1 + 1);
        int position3 = strIp.indexOf('.', position2 + 1);
        ip[0] = Long.parseLong(strIp.substring(0, position1));
        ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
        ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
        ip[3] = Long.parseLong(strIp.substring(position3 + 1));
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
    }

    public static String getLocalIpv4Address() {
        Collection<String> localIps = getLocalIpv4Addresses();
        Iterator<String> iterator = localIps.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

    public static Multimap<String, String> getConcreteLocalIpv4Addresses() {
        Multimap<String, String> result = ArrayListMultimap.create();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = allNetInterfaces.nextElement();

                boolean isConcreteAliveNetworkInterface = netInterface.isUp() && !netInterface.isLoopback() &&
                        !netInterface.isVirtual();
                if (!isConcreteAliveNetworkInterface) {
                    continue;
                }

                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        result.put(netInterface.getName(), ip.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            // ignore exception
            return result;
        }
        return result;
    }

    public static Collection<String> getLocalIpv4Addresses() {
        List<String> result = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = allNetInterfaces.nextElement();

                boolean isConcreteAliveNetworkInterface = netInterface.isUp() && !netInterface.isLoopback() &&
                        !netInterface.isVirtual();
                if (!isConcreteAliveNetworkInterface) {
                    continue;
                }

                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        result.add(ip.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            // ignore exception
            return result;
        }
        return result;
    }
}
