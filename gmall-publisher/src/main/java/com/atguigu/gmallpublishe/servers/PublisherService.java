package com.atguigu.gmallpublishe.servers;

import java.util.Map;

public interface PublisherService {

    public int getDauTotal(String date);

    public Map getDauTotalHours(String date);

    public Double getOrderAmountTotal(String date);

    public Map getOrderAmountHourMap(String date);
}
