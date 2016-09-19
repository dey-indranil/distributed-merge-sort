package com.punitivetoe;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class DistributedMergeLocalNodeApp {

	public static void main(String[] args) {
		SpringApplication.run(DistributedMergeLocalNodeApp.class, args);
	}
}

@Component
class NexonHealthIndicator implements HealthIndicator {

	@Override
	public Health health() {
		return Health.status("I do <3 Nexon!!").build();
	}
}

@RestController
@RefreshScope
class DistributedMergeRestController {

	@Autowired
	private Processor<Integer> processor;
	
	public DistributedMergeRestController() {
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/peek")
	public int peek() {
		return processor.getQueue().peek();
	}

	@RequestMapping(method = RequestMethod.GET, value = "/poll")
	public int poll() {
		return processor.getQueue().poll();
	}
}

@Component
class Processor<T>{
	private Queue<T> queue;
	public void process(List<T> elements, Comparator<T> comparator){
		Collections.sort(elements, comparator);//default implementation is optimized merge-sort
		queue = new ConcurrentLinkedQueue<T>(elements);
	}
	public Queue<T> getQueue() {
		return queue;
	}
	public void setQueue(Queue<T> queue) {
		this.queue = queue;
	}
	
}

@Component
class InputFileWatcherService{
	private static final String INPUT_DIR = "src/main/resources/input";
	private WatchService watcher;
	private List<SampleRecordsCLR> observers = new ArrayList<>();
	
	@Autowired
	private SampleRecordsCLR observer;
	
	public void init() throws IOException{
		Path dir = Paths.get(INPUT_DIR);
		watcher = FileSystems.getDefault().newWatchService();
		WatchKey key = dir.register(watcher,
                ENTRY_CREATE,
                ENTRY_DELETE,
                ENTRY_MODIFY);
		System.out.println(key.isValid());
		addObserver(observer);
	}
	
	private void addObserver(SampleRecordsCLR observer2) {
		observers.add(observer2);
	}

	public void startWatching(){
		new Thread(new Runnable(){

			@Override
			public void run() {
				event();
			}
			
		}).start();
	}
	
	public void event(){
		for (;;) {

		    // wait for key to be signaled
		    WatchKey key;
		    try {
		        key = watcher.take();
		    } catch (InterruptedException x) {
		        return;
		    }

		    for (WatchEvent<?> event: key.pollEvents()) {
		        WatchEvent.Kind<?> kind = event.kind();

		        // This key is registered only
		        // for ENTRY_CREATE events,
		        // but an OVERFLOW event can
		        // occur regardless if events
		        // are lost or discarded.
		        if (kind == OVERFLOW) {
		            continue;
		        }

		        // The filename is the
		        // context of the event.
		        @SuppressWarnings("unchecked")
				WatchEvent<Path> ev = (WatchEvent<Path>)event;
		        Path filename = ev.context();
		        File f = new File(INPUT_DIR+"/"+filename.getFileName());
		        List<Integer> fileContents = transmit(f.toPath());
		        notifyObservers(fileContents);
		    }

		    // Reset the key -- this step is critical if you want to
		    // receive further watch events.  If the key is no longer valid,
		    // the directory is inaccessible so exit the loop.
		    boolean valid = key.reset();
		    if (!valid) {
		        break;
		    }
		}
	}

	private void notifyObservers(List<Integer> fileContents) {
		for(SampleRecordsCLR c:observers){
			c.update(fileContents);
		}
	}

	private List<Integer> transmit(Path fileName) {
		List<String> strRes = null;
		//read file into stream, try-with-resources
		try (Stream<String> stream = Files.lines(Paths.get(fileName.toAbsolutePath().toUri()))) {
			//stream.forEach(System.out::println);
			strRes = stream.flatMap(Pattern.compile(",")::splitAsStream)
				    .collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Integer> intList = new ArrayList<>();
		strRes.forEach(a->intList.add(Integer.valueOf(a)));
		return intList;
	}

	public WatchService getWatcher() {
		return watcher;
	}

	public void setWatcher(WatchService watcher) {
		this.watcher = watcher;
	}

}

@Component
class SampleRecordsCLR implements CommandLineRunner{
	
	@Autowired
	private Processor<Integer> processor;
	
	@Autowired
	private InputFileWatcherService watchService;
	
	@Override
	public void run(String... args) throws Exception {
		watchService.init();
		watchService.startWatching();
	}
	
	public void update(List<Integer> fileContentList) {
		processor.process(fileContentList, new Comparator<Integer>() {
		    public int compare(Integer o1, Integer o2) {
		        return o2.compareTo(o1);
		    }
		});
		
	}
	
	
}


