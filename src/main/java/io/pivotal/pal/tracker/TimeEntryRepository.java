package io.pivotal.pal.tracker;

import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TimeEntryRepository {

    TimeEntry create(TimeEntry timeEntry);
    TimeEntry find(Long id);
    List<TimeEntry> list();
    TimeEntry update(Long id, TimeEntry timeEntry);
    TimeEntry delete(Long id);

    /*nMemoryTimeEntryRepository inMemoryTimeEntryRepository;

    public TimeEntry create(TimeEntry timeEntry) {

        return inMemoryTimeEntryRepository.create(timeEntry);
    }

    public TimeEntry find(long timeEntryId) {

        return inMemoryTimeEntryRepository.find(timeEntryId);
    }

    public List<TimeEntry> list() {
        return inMemoryTimeEntryRepository.list();
    }

    public TimeEntry update(long eq, TimeEntry any) {
        return inMemoryTimeEntryRepository.update(eq,any);
    }

    public TimeEntry delete(long timeEntryId) {

        return inMemoryTimeEntryRepository.delete(timeEntryId);
    }*/
}
