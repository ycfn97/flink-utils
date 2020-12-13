package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: bean
 * ClassName: OrderInfo01
 *
 * @author 18729 created on date: 2020/12/10 12:29
 * @version 1.0
 * @since JDK 1.8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderInfo01 {
    private Long id;
    private Long province_id;
    private String order_status;
    private Long user_id;
    private Double final_total_amount;
    private Double benefit_reduce_amount;
    private Double original_total_amount;
    private Double feight_fee;
    private String expire_time;
    private String create_time;
    private String operate_time;
    private String create_date;
    private String create_hour;
    private String if_first_order;
    private String province_name;
    private String province_area_code;
    private String province_iso_code;
    private String province_iso_3166_2;
    private String user_age_group;
    private String user_gender;
}
