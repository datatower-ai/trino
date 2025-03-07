/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.datatower.functions.util;

/**
 * @author liulin
 */
public class BinaryPrintUtil
{
    private static final long[] arr = new long[64];

    private BinaryPrintUtil()
    {
    }

    /**
     * 打印a的二进制表示
     *
     * @param a 要打印的数字
     * @param removeFrontZero 是否移除前端多余的0
     * @param space 间隔多少位空一格 -1 表示没有间隔,最大值为32
     */
    public static String show(long a, boolean removeFrontZero, int space)
    {
        return show(a, 0, removeFrontZero, space);
    }

    /**
     * 打印a的二进制表示
     *
     * @param a 要打印的数字
     * @param removeFrontZero 是否移除前端多余的0
     * @param space 间隔多少位空一格 -1 表示没有间隔,最大值为32
     */
    public static String show(int a, boolean removeFrontZero, int space)
    {
        return show(a, 32, removeFrontZero, space);
    }

    /**
     * 打印a的二进制表示
     *
     * @param a 要打印的数字
     * @param removeFrontZero 是否移除前端多余的0
     * @param space 间隔多少位空一格 -1 表示没有间隔,最大值为32
     */
    public static String show(short a, boolean removeFrontZero, int space)
    {
        return show(a, 48, removeFrontZero, space);
    }

    /**
     * 打印a的二进制表示
     *
     * @param a 要打印的数字
     * @param removeFrontZero 是否移除前端多余的0
     * @param space 间隔多少位空一格 -1 表示没有间隔,最大值为32
     */
    public static String show(byte a, boolean removeFrontZero, int space)
    {
        return show(a, 56, removeFrontZero, space);
    }

    public static void show(long a)
    {
        System.out.println(show(a, 0, false, 8));
    }

    public static void show(int a)
    {
        System.out.println(show(a, 0, false, 8));
    }

    public static String show(short a)
    {
        return show(a, 0, false, 1);
    }

    public static String show(byte a)
    {
        return show(a, 0, false, 1);
    }

    /**
     * 打印a的二进制表示
     *
     * @param removeFrontZero 是否移除前端多余的0
     * @param space 间隔多少位空一格 -1 表示没有间隔,最大值为32
     */
    public static String show(long a, int beginIndex, boolean removeFrontZero, int space)
    {
        //打印a的二进制数据
        StringBuilder builder = new StringBuilder();
        if (space > 32) {
            space = -1;
        }
        for (int i = beginIndex; i < arr.length; i++) {
            if ((a & arr[i]) == arr[i]) {
                builder.append("1");
            }
            else {
                builder.append("0");
            }
            if (space != -1 && i != 0 && (i + 1) % space == 0) {
                builder.append(" ");
            }
        }
        String result = builder.toString();
        if (removeFrontZero) {
            char[] s = builder.toString().toCharArray();
            int subStringBeginIndex = 0;
            for (int i = 0; i < s.length; i++) {
                if (s[i] != '1') {
                    continue;
                }
                subStringBeginIndex = i;
                if (space != -1) {
                    //找到了第一个非0数，开始向前搜索第一个空格
                    for (int j = subStringBeginIndex; j >= 0; j--) {
                        if (s[j] == ' ') {
                            subStringBeginIndex = j;
                            break;
                        }
                    }
                }
            }
            result = result.substring(subStringBeginIndex);
        }
        return result;
    }

    static {
        arr[0] = 0x8000000000000000L;
        for (int i = 1; i < arr.length; i++) {
            arr[i] = arr[i - 1] >>> 1;
        }
    }
}
