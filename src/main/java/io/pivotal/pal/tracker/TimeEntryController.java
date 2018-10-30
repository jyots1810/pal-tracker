package io.pivotal.pal.tracker;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/time-entries")
public class TimeEntryController {

    @Autowired
    TimeEntryRepository timeEntryRepository;

    public TimeEntryController(TimeEntryRepository timeEntryRepository) {
        this.timeEntryRepository = timeEntryRepository;
    }

    @PostMapping
    @ResponseBody
    public ResponseEntity<TimeEntry> create(@RequestBody TimeEntry timeEntryToCreate) {
        return new ResponseEntity(timeEntryRepository.create(timeEntryToCreate), HttpStatus.CREATED);
    }

    @GetMapping("/{timeEntryId}")
    @ResponseBody
    public ResponseEntity<TimeEntry> read(@PathVariable long timeEntryId) {
        TimeEntry foundTimeEntry = timeEntryRepository.find(timeEntryId);
        if(foundTimeEntry!=null)
            return new ResponseEntity(foundTimeEntry, HttpStatus.OK);
        else
            return new ResponseEntity(foundTimeEntry, HttpStatus.NOT_FOUND);
    }

    @GetMapping
    @ResponseBody
    public ResponseEntity<List<TimeEntry>> list() {
        return new ResponseEntity(timeEntryRepository.list(), HttpStatus.OK);
    }

    @PutMapping("/{timeEntryId}")
    @ResponseBody
    public ResponseEntity update(@PathVariable long timeEntryId, @RequestBody TimeEntry expected) {

        TimeEntry timeEntryUpdated = timeEntryRepository.update(timeEntryId, expected);
        if(timeEntryUpdated!=null)
            return new ResponseEntity(timeEntryUpdated, HttpStatus.OK);
        else
            return new ResponseEntity(HttpStatus.NOT_FOUND);
    }

    @DeleteMapping("/{timeEntryId}")
    @ResponseBody
    public ResponseEntity<TimeEntry> delete(@PathVariable long timeEntryId) {
       return new ResponseEntity(timeEntryRepository.delete(timeEntryId), HttpStatus.NO_CONTENT);
    }
}
