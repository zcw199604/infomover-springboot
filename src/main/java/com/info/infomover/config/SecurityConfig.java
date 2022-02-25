package com.info.infomover.config;

import com.info.baymax.common.utils.JsonUtils;
import com.info.infomover.entity.User;
import com.info.infomover.jwt.JwtAuthenticationTokenFilter;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.service.UserService;
import com.info.infomover.util.BCryptPasswordEncoderUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.AuthenticationEntryPoint;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableWebSecurity //启用security
@EnableGlobalMethodSecurity(prePostEnabled=true)
public class SecurityConfig extends WebSecurityConfigurerAdapter{

    @Autowired
    private UserService userService;

    @Autowired
    private UserRepository userRepository;

    @Override
    protected void configure(HttpSecurity httpSecurity) throws Exception {
        /*httpSecurity.cors().disable().csrf()// 由于使用的是JWT，我们这里不需要csrf 进行关闭
                .disable()
                .sessionManagement()// 基于token，所以不管理Session
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .authorizeRequests()
                .antMatchers("/api/user/login","/api/user")// 对登录注册要允许匿名访问
                .permitAll()
                .antMatchers(HttpMethod.OPTIONS)//跨域请求会先进行一次options请求
//                .permitAll()
                //.antMatchers("/**")//测试时全部运行访问
                .permitAll()
                .anyRequest()// 除上面外的所有请求全部需要鉴权认证
                .authenticated();
        // 禁用缓存
        httpSecurity.headers().cacheControl();
        // 添加自定义 JWT 过滤器
        httpSecurity.addFilterBefore(jwtAuthenticationTokenFilter(), UsernamePasswordAuthenticationFilter.class);*/
        //添加自定义未授权和未登录结果返回


        httpSecurity.authorizeRequests().anyRequest().anonymous().and()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .formLogin()
                //.loginPage("/login/toLogin")
                .loginProcessingUrl("/api/user/toDoLogin")
                .usernameParameter("username")
                //.defaultSuccessUrl("/case/toList",true)
                .and()
                /*.logout()
                .logoutUrl("/logout")
                .logoutSuccessUrl("/login/toLogin")
                .deleteCookies()
                .permitAll()
                .and()*/
                .csrf().disable()
                .exceptionHandling()
                .authenticationEntryPoint(new AuthenticationEntryPoint() {
                    @Override
                    public void commence(HttpServletRequest req, HttpServletResponse resp, AuthenticationException authException) throws IOException, ServletException {
                        resp.setContentType("application/json;charset=utf-8");
                        PrintWriter out = resp.getWriter();

                        out.write(JsonUtils.toJson(Response.status(Response.Status.UNAUTHORIZED).build()));
                        out.flush();
                        out.close();
                    }
                });
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(userDetailsService())
                .passwordEncoder(passwordEncoder());
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoderUtil();
    }

    /**
     * 解决 无法直接注入 AuthenticationManager
     *
     * @return
     * @throws Exception
     */
    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception
    {
        return super.authenticationManagerBean();
    }

    @Bean
    public UserDetailsService userDetailsService() {
        //获取登录用户信息
        return new UserDetailsService() {
            @Override
            public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
                //根据用户名获取用户
                User admin = userRepository.findByName(username);
                if (admin != null) {
                    //获取用户的所有权限
                    //List<UmsPermission> permissionList = adminService.getPermissionList(admin.getId());
                    //返回自己实现的用户用户信息*//*
                    String role = admin.getRole();
                    List<SimpleGrantedAuthority> authority = new ArrayList<>();
                    authority.add(new SimpleGrantedAuthority(role));

                    return new org.springframework.security.core.userdetails.User(admin.getName(),admin.getPassword(),authority);
                }
                throw new UsernameNotFoundException("用户名或密码错误");
            }
        };
    }

    /**
     * 注册自定义JWT登录授权过滤器
     *
     * @return JwtAuthenticationTokenFilter
     */

    @Autowired
    private JwtAuthenticationTokenFilter jwtAuthenticationTokenFilter() {
        return new JwtAuthenticationTokenFilter();
    }

    /*@Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }*/


    @Override
    public void configure(WebSecurity web) throws Exception {
        web.ignoring().antMatchers("/api/user/toDoLogin");
    }
}