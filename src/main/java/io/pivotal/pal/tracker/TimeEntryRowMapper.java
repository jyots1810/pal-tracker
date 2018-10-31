package io.pivotal.pal.tracker;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;

public class TimeEntryRowMapper implements RowMapper {
    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
        TimeEntry timeEntry = new TimeEntry();
        timeEntry.setId(rs.getInt("ID"));
        timeEntry.setDate(rs.getDate("DATE").toLocalDate());
        timeEntry.setHours(rs.getInt("HOURS"));
        timeEntry.setProjectId(rs.getInt("PROJECT_ID"));
        timeEntry.setUserId(rs.getInt("USER_ID"));
        return timeEntry;
    }
}
