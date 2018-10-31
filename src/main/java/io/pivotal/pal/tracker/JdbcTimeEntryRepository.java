package io.pivotal.pal.tracker;

import com.mysql.cj.api.jdbc.Statement;

import java.sql.*;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class JdbcTimeEntryRepository implements TimeEntryRepository {


    private JdbcTemplate jdbcTemplate;
    private RowMapper<TimeEntry> timeEntryRowMapper = new RowMapper<TimeEntry>() {
        @Override
        public TimeEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
            TimeEntry timeEntry = new TimeEntry();
            timeEntry.setId(rs.getInt("ID"));
            timeEntry.setDate(rs.getDate("DATE").toLocalDate());
            timeEntry.setHours(rs.getInt("HOURS"));
            timeEntry.setProjectId(rs.getInt("PROJECT_ID"));
            timeEntry.setUserId(rs.getInt("USER_ID"));
            return timeEntry;
        }
    };

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
            List<TimeEntry> timeEntries = jdbcTemplate.query("Select * from time_entries where id = ?", timeEntryRowMapper, id);
            if(timeEntries != null && timeEntries.size() > 0) {
                result = timeEntries.get(0);
            }
        }
        catch(EmptyResultDataAccessException e) {}
        return result;
    }

    @Override
    public List<TimeEntry> list() {
        List<TimeEntry> result = new ArrayList<TimeEntry>();
        try {
            result = jdbcTemplate.query("Select * from time_entries", timeEntryRowMapper);
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
