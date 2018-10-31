package io.pivotal.pal.tracker;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/time-entries")
public class TimeEntryController {

    private final CounterService counterService;
    private final GaugeService gaugeService;
    TimeEntryRepository timeEntryRepository;

    public TimeEntryController(TimeEntryRepository timeEntryRepository, CounterService counterService, GaugeService gaugeService) {
        this.timeEntryRepository = timeEntryRepository;
        this.counterService = counterService;
        this.gaugeService = gaugeService;
    }

    @PostMapping
    @ResponseBody
    public ResponseEntity<TimeEntry> create(@RequestBody TimeEntry timeEntryToCreate) {
        counterService.increment("TimeEntry.created");
        gaugeService.submit("TimeEntry.count", timeEntryRepository.list().size());
        return new ResponseEntity(timeEntryRepository.create(timeEntryToCreate), HttpStatus.CREATED);
    }

    @GetMapping("/{timeEntryId}")
    @ResponseBody
    public ResponseEntity<TimeEntry> read(@PathVariable long timeEntryId) {
        counterService.increment("TimeEntry.read");
        TimeEntry foundTimeEntry = timeEntryRepository.find(timeEntryId);
        if(foundTimeEntry!=null)
            return new ResponseEntity(foundTimeEntry, HttpStatus.OK);
        else
            return new ResponseEntity(foundTimeEntry, HttpStatus.NOT_FOUND);
    }

    @GetMapping
    @ResponseBody
    public ResponseEntity<List<TimeEntry>> list() {
        counterService.increment("TimeEntry.listed");
        return new ResponseEntity(timeEntryRepository.list(), HttpStatus.OK);
    }

    @PutMapping("/{timeEntryId}")
    @ResponseBody
    public ResponseEntity update(@PathVariable long timeEntryId, @RequestBody TimeEntry expected) {

        counterService.increment("TimeEntry.updated");
        TimeEntry timeEntryUpdated = timeEntryRepository.update(timeEntryId, expected);
        if(timeEntryUpdated!=null)
            return new ResponseEntity(timeEntryUpdated, HttpStatus.OK);
        else
            return new ResponseEntity(HttpStatus.NOT_FOUND);
    }

    @DeleteMapping("/{timeEntryId}")
    @ResponseBody
    public ResponseEntity<TimeEntry> delete(@PathVariable long timeEntryId) {
        counterService.increment("TimeEntry.deleted");
        gaugeService.submit("TimeEntry.count", timeEntryRepository.list().size());
       return new ResponseEntity(timeEntryRepository.delete(timeEntryId), HttpStatus.NO_CONTENT);
    }
}
