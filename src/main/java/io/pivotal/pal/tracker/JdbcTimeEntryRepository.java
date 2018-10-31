package io.pivotal.pal.tracker;

import com.mysql.cj.api.jdbc.Statement;
import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.*;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcTimeEntryRepository implements TimeEntryRepository {


    private JdbcTemplate jdbcTemplate;

    public JdbcTimeEntryRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public TimeEntry create(TimeEntry timeEntry) {
        final PreparedStatementCreator psc = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
                final PreparedStatement ps = connection.prepareStatement("INSERT INTO time_entries (project_id, user_id, date, hours) VALUES (?, ?, ?, ?)",
                        Statement.RETURN_GENERATED_KEYS);
                ps.setLong(1, timeEntry.getProjectId());
                ps.setLong(2,timeEntry.getUserId());
                ps.setDate(3, Date.valueOf(timeEntry.getDate()));
                ps.setInt(4,timeEntry.getHours());
                return ps;
            }
        };



        final KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(psc,holder);
        timeEntry.setId(holder.getKey().longValue());
        return timeEntry;
    }

    @Override
    public TimeEntry find(Long id) {
        TimeEntry result = null;
        try {
            result = (TimeEntry) jdbcTemplate.queryForObject("Select * from time_entries where id = ?", new Object[]{id}, new TimeEntryRowMapper());
        }
        catch(EmptyResultDataAccessException e) {}
        return result;
    }

    @Override
    public List<TimeEntry> list() {
        List<TimeEntry> result = new ArrayList<TimeEntry>();
        try {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList("Select * from time_entries");
            rows.stream().forEach((row) -> {
                TimeEntry timeEntry = new TimeEntry();
                timeEntry.setId((long)row.get("id"));
                timeEntry.setProjectId((long)row.get("project_id"));
                timeEntry.setHours((int)row.get("hours"));
                timeEntry.setDate(((Date)row.get("date")).toLocalDate());
                timeEntry.setUserId((long)row.get("user_id"));
                result.add(timeEntry);
            });

        }
        catch(EmptyResultDataAccessException e) {}
        return result;
    }

    @Override
    public TimeEntry update(Long id, TimeEntry timeEntry) {
        jdbcTemplate.update("update time_entries set project_id = ?, user_id = ?, date = ?, hours = ? where id = ?",
                timeEntry.getProjectId(), timeEntry.getUserId(), timeEntry.getDate(), timeEntry.getHours(), id);
        timeEntry.setId(id);
        return timeEntry;
    }

    @Override
    public TimeEntry delete(Long id) {
        TimeEntry result = find(id);
        jdbcTemplate.update("DELETE FROM time_entries WHERE id = ?", id);
        return result;
    }
}
