package com.info.infomover.resources;

import com.info.infomover.entity.Page;
import com.info.infomover.entity.QUser;
import com.info.infomover.entity.User;
import com.info.infomover.jwt.TokenUtils;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.service.UserService;
import com.info.infomover.util.AesUtils;
import com.info.infomover.util.CheckUtil;
import com.info.infomover.util.UserUtil;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.annotation.security.RolesAllowed;
import javax.ws.rs.core.Response;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RequestMapping("/api/user")
@Controller
@ResponseBody
//@Tag(name = "infomover user", description = "用户管理接口")
public class UserResource {
    private static Logger logger = LoggerFactory.getLogger(UserResource.class);

    @Value("${login.token.expiration}")
    private Long duration;

    @Value("${mp.jwt.verify.issuer}")
    public String issuer;

    @Autowired
    @Qualifier("infomover_user_service")
    private UserService userService;

    @Autowired
    private JPAQueryFactory queryFactory;

    @Autowired
    private UserRepository userRepository;

    @GetMapping
    @RolesAllowed({"Admin"})
    /*@Operation(description = "用户列表")
    @Parameters({
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
            @Parameter(name = "status", description = "用户状态", in = ParameterIn.QUERY),
            @Parameter(name = "username", description = "用户名字", in = ParameterIn.QUERY),
            @Parameter(name = "chinesename", description = "中文名", in = ParameterIn.QUERY),
            @Parameter(name = "role", description = "角色", in = ParameterIn.QUERY),
            @Parameter(name = "after", description = "创建时间大于", in = ParameterIn.QUERY),
            @Parameter(name = "before", description = "创建时间小于", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "排序字段", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "排序方向", in = ParameterIn.QUERY),
    })*/
    public Response list(@RequestParam(value = "pageNum",defaultValue = "1",required = false) int page, @RequestParam(value = "pageSize",defaultValue = "10",required = false) int limit,
                         @RequestParam(value = "status", required = false) List<User.Status> status, @RequestParam(value = "username", required = false) String userName,
                         @RequestParam(value = "chinesename", required = false) String chineseName, @RequestParam(value = "role", required = false) String role,
                         @RequestParam(value = "after", required = false) String createAfter, @RequestParam(value = "before", required = false) String createBefore,
                         @RequestParam(value = "sortby",defaultValue = "createTime",required = false) String sortBy,
                         @RequestParam(value = "sortdirection",defaultValue = "DESC",required = false) Sort.Direction sortDirection) {
        QUser user = QUser.user;
        JPAQuery<User> from = queryFactory.select(user).from(user);
        if (status != null && !status.isEmpty()) {
            from.where(user.status.in(status));
        }
        if (!StringUtils.isEmpty(userName)) {
            from.where(user.name.like("%" + userName + "%"));
        }
        if (!StringUtils.isEmpty(chineseName)) {
            from.where(user.chineseName.like("%" + chineseName + "%"));
        }
        if (!StringUtils.isEmpty(role)) {
            from.where(user.role.eq(role));
        }
        if (!StringUtils.isEmpty(createAfter)) {
            from.where(user.createTime.after(LocalDate.parse(createAfter).atStartOfDay()));
        }
        if (!StringUtils.isEmpty(createBefore)) {
            from.where(user.createTime.before(LocalDate.parse(createAfter).atStartOfDay()));
        }

        Order order = sortDirection.isAscending() ? Order.ASC : Order.DESC;
        Path<Object> fieldPath = com.querydsl.core.types.dsl.Expressions.path(Object.class, user, sortBy);
        from.orderBy(new OrderSpecifier(order, fieldPath));
        QueryResults<User> userQueryResults = from.offset(Page.getOffest(page, limit)).limit(limit).fetchResults();


        Page of = Page.of(userQueryResults.getResults(), userQueryResults.getTotal(), userQueryResults.getLimit());
        return Response.ok(of).build();
    }

    @GetMapping("/{userid}")
    /*@RolesAllowed({"Admin", "User"})
    @Operation(description = "用户详情")
    @Parameters({
            @Parameter(name = "userid", description = "用户id", required = true, in = ParameterIn.QUERY),
    })*/
    public Response getUser(@PathVariable("userid") Long uid) {
        User user = userRepository.findById(uid).get();
        if (user == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        if ("admin".equalsIgnoreCase(user.getRole()) && !UserUtil.getUserName().equals(user.name)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        return Response.ok(user).build();
    }

    @PostMapping("/login")
    public Response login(@RequestParam("username") String name, @RequestParam("password") String pwd) {
        User user = userRepository.findByName(name);
        if (user != null && user.status == User.Status.ENABLED) {
            try {
                if (userService.verifyBCryptPassword(AesUtils.wrappeDaesDecrypt(pwd), user.password)) {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("token", TokenUtils.generateToken(user.name, Set.of(user.role), duration, issuer));
                    map.put("role", user.role);
                    map.put("expiresIn", duration);
                    map.put("id", user.getId());
                    return Response.ok(map).build();
                } else {
                    throw new RuntimeException("wrong user name or password");
                }
            } catch (Exception e) {
                return Response.status(HttpStatus.UNAUTHORIZED.value()).build();
            }
        } else {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
    }

    @Resource
    private AuthenticationManager authenticationManager;

    @PostMapping("/toDoLogin")
    public Response toDoLogin(@RequestParam("username") String name, @RequestParam("password") String pwd) {
        // 用户验证
        Authentication authentication = null;
        try {
            // 该方法会去调用UserDetailsServiceImpl.loadUserByUsername
            authentication = authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(name, pwd));
        } catch (Exception e) {
            if (e instanceof BadCredentialsException) {
                return Response.status(Response.Status.UNAUTHORIZED).build();
            } else {

                return Response.status(Response.Status.UNAUTHORIZED).build();
            }
        }
        org.springframework.security.core.userdetails.User user = (org.springframework.security.core.userdetails.User) authentication.getPrincipal();

        try {
            Set<String> collect = user.getAuthorities().stream().map(item -> item.getAuthority()).collect(Collectors.toSet());
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("token", TokenUtils.generateToken(user.getUsername(), collect, duration, issuer));
            map.put("role", collect);
            map.put("expiresIn", duration);
//            map.put("id", user);
            return Response.ok(map).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(Response.Status.UNAUTHORIZED).build();
    }

    @PostMapping("/renew")
    @RolesAllowed({"User", "Admin"})
    public Response renew() {
        Map<String, Object> map = new HashMap<String, Object>();
        try {
            String curUserName = UserUtil.getUserName();
            User user = userRepository.findByName(curUserName);
            if (user != null && user.status == User.Status.ENABLED) {
                map.put("token", TokenUtils.generateToken(user.name, Set.of(user.role), duration, issuer));
                map.put("role", user.role);
                map.put("expiresIn", duration);
                return Response.ok(map).build();
            } else {
                throw new RuntimeException("user " + curUserName + " not available.");
            }
        } catch (Exception e) {
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @PostMapping
    @Transactional
    public Response register(@RequestParam("username") String name, @RequestParam("chinesename") String chineseName,
                             @RequestParam("password") String pwd, @RequestParam("email") String email, @RequestParam("note") String note) {
        User existUser = userRepository.findByName(name);
        if (existUser != null) {
            throw new RuntimeException("user with name " + name + " already exists");
        }
        long count = userRepository.countByChineseName(chineseName);
        CheckUtil.checkTrue(count > 0, "user with chineseName " + chineseName + " already exists");
        try {
            userService.add(name, chineseName, AesUtils.wrappeDaesDecrypt(pwd), User.Role.User.name(), email, note);
            return Response.ok().build();
        } catch (Exception e) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @PostMapping("/password")
    @RolesAllowed({"User", "Admin"})
    @Transactional
    public Response changePwd(@RequestParam("userid") long uid, @RequestParam("oldPass") String oldPwd, @RequestParam("newPass") String newPwd) {
        try {
            String curUserName = UserUtil.getUserName();
            userService.updatePassword(curUserName, uid, AesUtils.wrappeDaesDecrypt(oldPwd), AesUtils.wrappeDaesDecrypt(newPwd));
            return Response.ok().build();
        } catch (Exception e) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @PostMapping("/email")
    @RolesAllowed({"Admin", "User"})
    @Transactional
    public Response changeEmail(@RequestParam("userid") Long uid, @RequestParam("email") String email) {
        try {
            String curUserName = UserUtil.getUserName();
            userService.updateEmail(curUserName, uid, email);
            return Response.ok().build();
        } catch (Exception e) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @PostMapping("/admin")
    @RolesAllowed("Admin")
    @Transactional
    public Response enableAdmin(@RequestParam("userid") Long uid, @RequestParam("isAdmin") boolean isAdmin) {
        try {
            User user = userRepository.findById(uid).get();
            if ("admin".equalsIgnoreCase(user.getRole())) {
                throw new RuntimeException("wrong operation with user " + UserUtil.getUserName());
            }
            userService.enableAdmin(uid, isAdmin);
            return Response.ok().build();
        } catch (Exception e) {
            Map<String, Object> map = new HashMap<>();
            map.put("err", e.getMessage());
            return Response.status(HttpStatus.BAD_REQUEST.value()).entity(map).build();
        }
    }

    @PostMapping("/enable")
    @RolesAllowed("Admin")
    @Transactional
    public Response enableUser(@RequestParam("userid") String uid, @RequestParam("enable") boolean enable) {
        try {
            User user = userRepository.findById(Long.valueOf(uid)).get();
            if ("admin".equalsIgnoreCase(user.getRole())) {
                throw new RuntimeException("wrong operation with user " + UserUtil.getUserName());
            }
            userService.enableUser(Long.valueOf(uid), enable);
            return Response.ok().build();
        } catch (Exception e) {
            Map<String, Object> map = new HashMap<>();
            map.put("err", e.getMessage());
            return Response.status(HttpStatus.BAD_REQUEST.value()).entity(map).build();
        }
    }
}
