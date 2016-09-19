package com.punitivetoe;

import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class DistributedMergeCoordinatorApp {

	public static void main(String[] args) {
		SpringApplication.run(DistributedMergeCoordinatorApp.class, args);
	}
}

@RestController
@RefreshScope
class DistributedMergeCoordRestController {

	@Autowired
	private ProcessorCoordinator processor;
	
	public DistributedMergeCoordRestController() {
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/poll")
	public int poll() {
		return -1;
	}
}

@Component
class ProcessorCoordinator{
	
}

@Component
class DMCoordinatorCLR implements CommandLineRunner{
	
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


