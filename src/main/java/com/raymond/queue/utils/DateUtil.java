package com.raymond.queue.utils;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 时间工具类
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2019-12-11 12:48
 */
@SuppressWarnings("all")
public class DateUtil {
    private static Map<String, SimpleDateFormat> map = new HashMap<String, SimpleDateFormat>();
    static {
        DateStyle[] values = DateStyle.values();
        SimpleDateFormat dateFormat;
        for (DateStyle style : values) {
            dateFormat = new SimpleDateFormat(style.getValue());
            map.put(style.getValue(), dateFormat);
        }
    }

    /**
     * 获取SimpleDateFormat
     *
     * @param style
     *            日期格式
     * @return SimpleDateFormat对象
     * @throws RuntimeException
     *             异常：非法日期格式
     */
    public static SimpleDateFormat getDateFormat(DateStyle style) {
        return map.get(style.getValue());
    }

    /**
     * 获取日期中的某数值。如获取月份
     *
     * @param date
     *            日期
     * @param dateType
     *            日期格式
     * @return 数值
     */
    public static int getInteger(Date date, int dateType) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(dateType);
    }

    /**
     * 使用yyyy-MM-dd HH:mm:ss格式提取字符串日期
     *
     * @param strDate
     *            日期字符串
     * @return date
     */
    public static Date parse(String strDate) throws Exception{
        return parse(strDate, DateStyle.YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 使用指定格式提取字符串日期
     *
     * @param strDate
     *            日期字符串
     * @param style
     *            日期格式
     * @return date
     */
    public static Date parse(String strDate, DateStyle style) throws Exception{
        return getDateFormat(style).parse(strDate);
    }

    /**
     * 和当前时间比较大小
     *
     * @param date date
     * @return
     */
    public static int compareDateWithNow(Date date) throws Exception{
        Date nowDate = new Date();
        return date.compareTo(nowDate);
    }

    /**
     * 获取系统当前时间
     *
     * @return 指定格式的字符串
     */
    public static String getNowTime() throws Exception{
        return getNowTime(DateStyle.YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 获取系统当前时间
     *
     * @return 指定格式的字符串
     */
    public static String getNowTime(DateStyle pattern) {
        SimpleDateFormat formatter = getDateFormat(pattern);
        return formatter.format(new Date());
    }

    /**
     * 将时间转换为指定格式的字符串
     * @param date date
     * @return 指定格式的字符串
     */
    public static String dateToStr(Date date) {
        return dateToStr(date, DateStyle.YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 将时间转换为指定格式的字符串
     * @param date date
     * @param pattern 字符串格式
     * @return 指定格式的字符串
     */
    public static String dateToStr(Date date,DateStyle pattern) {
        return getDateFormat(pattern).format(date);
    }

    /**
     * 时间格式字符串转化成另一种时间格式字符串
     * @param timeStr 时间字符串
     * @return 年月日时分秒的时间字符串
     * @throws Exception 异常
     */
    public static String parseTime(String timeStr) throws Exception{
        return parseTime(timeStr, DateStyle.YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 时间格式字符串转化成另一种时间格式字符串
     * @param timeStr 时间字符串
     * @param pattern 需要转换的时间格式
     * @return 转换的时间字符串
     * @throws Exception 异常
     */
    public static String parseTime(String timeStr ,DateStyle pattern) throws Exception{
        SimpleDateFormat formatter = getDateFormat(pattern);
        return formatter.format(formatter.parse(timeStr));
    }

    public static Date offset(Date date, int offset) {
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, offset);
        return calendar.getTime();
    }

    public static void main(String[] args) throws Exception {
        Date offset = offset(new Date(), -8);
        System.out.println(dateToStr(offset, DateStyle.YYYY_MM_DD));
    }
    /**
     * 时间格式枚举
     *
     * @author :  raymond
     * @version :  V1.0
     * @date :  2019-12-14 09:07
     */
    public enum DateStyle {
        /**月日**/
        MM_DD("MM-dd"),
        /**年月**/
        YYYY_MM("yyyy-MM"),
        /**年月日**/
        YYYY_MM_DD("yyyy-MM-dd"),
        /**月日时分**/
        MM_DD_HH_MM("MM-dd HH:mm"),
        /**月日时分秒**/
        MM_DD_HH_MM_SS("MM-dd HH:mm:ss"),
        /**年月日时分**/
        YYYY_MM_DD_HH_MM("yyyy-MM-dd HH:mm"),
        /**年月日时分秒**/
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        /**年月日时分秒毫秒**/
        YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS"),

        /**月日**/
        MM_DD_EN("MM/dd"),
        /**年月**/
        YYYY_MM_EN("yyyy/MM"),
        /**年月日**/
        YYYY_MM_DD_EN("yyyy/MM/dd"),
        /**月日时分**/
        MM_DD_HH_MM_EN("MM/dd HH:mm"),
        /**月日时分秒**/
        MM_DD_HH_MM_SS_EN("MM/dd HH:mm:ss"),
        /**年月日时分**/
        YYYY_MM_DD_HH_MM_EN("yyyy/MM/dd HH:mm"),
        /**年月日时分秒**/
        YYYY_MM_DD_HH_MM_SS_EN("yyyy/MM/dd HH:mm:ss"),
        /**月日**/
        MM_DD_CN("MM月dd日"),
        /**年月**/
        YYYY_MM_CN("yyyy年MM月"),
        /**年月日**/
        YYYY_MM_DD_CN("yyyy年MM月dd日"),
        /**月日时分**/
        MM_DD_HH_MM_CN("MM月dd日 HH:mm"),
        /**月日时分秒**/
        MM_DD_HH_MM_SS_CN("MM月dd日 HH:mm:ss"),
        /**年月日时分**/
        YYYY_MM_DD_HH_MM_CN("yyyy年MM月dd日 HH:mm"),
        /**年月日时分秒**/
        YYYY_MM_DD_HH_MM_SS_CN("yyyy年MM月dd日 HH:mm:ss"),

        /**时分**/
        HH_MM("HH:mm"),
        /**时分秒**/
        HH_MM_SS("HH:mm:ss");

        private String value;

        DateStyle(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
