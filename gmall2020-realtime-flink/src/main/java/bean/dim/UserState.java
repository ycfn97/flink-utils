package bean.dim;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: bean.dim
 * ClassName: UserState
 *
 * @author 18729 created on date: 2020/12/13 17:46
 * @version 1.0
 * @since JDK 1.8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserState {
    private String userId;
    private String ifConsumed;
}
