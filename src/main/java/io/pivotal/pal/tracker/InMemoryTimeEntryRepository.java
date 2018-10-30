package io.pivotal.pal.tracker;

import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class InMemoryTimeEntryRepository implements TimeEntryRepository{
    private Map<Long, TimeEntry> timeEntries = new HashMap<Long, TimeEntry>();
    private long id = 1L;

    @Override
    public TimeEntry create(TimeEntry timeEntry) {
        timeEntry.setId(id);
        id++;
        timeEntries.put(timeEntry.getId(), timeEntry);
        return timeEntry;
    }
    @Override
    public TimeEntry find(Long id) {
        return timeEntries.get(id);
    }
    @Override
    public List<TimeEntry> list() {
        return new ArrayList<>(timeEntries.values());
    }
    @Override
    public TimeEntry update(Long id, TimeEntry timeEntry) {
        if(find(id)!=null) {
            timeEntry.setId(timeEntries.get(id).getId());
            timeEntries.put(id, timeEntry);
            return timeEntry;
        }
        return null;
    }
    @Override
    public TimeEntry delete(Long id) {
        return timeEntries.remove(id);
    }
}
