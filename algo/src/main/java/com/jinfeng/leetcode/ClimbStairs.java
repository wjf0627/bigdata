package com.jinfeng.leetcode;

/**
 * @package: com.jinfeng.leetcode
 * @author: wangjf
 * @date: 2020/10/12
 * @time: 4:22 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class ClimbStairs {
    public static int climbStairs(int n) {
        if (n == 1) {
            return 1;
        }
        int a = 1, b = 2;
        int c;
        for (int i = 3; i <= n; ++i) {
            c = a + b;
            a = b;
            b = c;
        }
        return b;
    }

    public static void main(String[] args) {
        System.out.println(climbStairs(10));
    }
}
