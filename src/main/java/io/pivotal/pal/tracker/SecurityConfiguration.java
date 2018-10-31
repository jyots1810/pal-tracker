package io.pivotal.pal.tracker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfiguration;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@EnableWebSecurity
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {

    private boolean disableHttps;

    public SecurityConfiguration(@Value("${HTTPS_DISABLED}") boolean disableHttps) {
        super(disableHttps);
        this.disableHttps = disableHttps;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {

        if(!disableHttps) {
            http.authorizeRequests().antMatchers("/**").hasRole("USER").and().formLogin()
                    .and().requiresChannel().anyRequest().requiresSecure();
        }
        http.authorizeRequests()
                .anyRequest().authenticated()
                .and()
                .formLogin()
                .and()
                .httpBasic().and().csrf().disable();
        http.anonymous().disable()
                .exceptionHandling()
                .authenticationEntryPoint(new org.springframework.boot.autoconfigure.security.Http401AuthenticationEntryPoint("headerValue"));

    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {

        auth.inMemoryAuthentication()
                .withUser("user").password("password").roles("USER");
    }
}
