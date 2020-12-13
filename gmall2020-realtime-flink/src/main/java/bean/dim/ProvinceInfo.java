package bean.dim;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: bean.dim
 * ClassName: ProvinceInfo
 *
 * @author 18729 created on date: 2020/12/13 12:51
 * @version 1.0
 * @since JDK 1.8
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ProvinceInfo {
    private String id;
    private String name;
    private String region_id;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
}
