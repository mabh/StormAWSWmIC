/*
* Copyright (c) 2013-2014 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package com.sag.tn.storm.stormmaven.spoutsources;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;

/*
 * reads xml files 1 by 1 files from a directory
 * create VTD object from it and returns a tuple
 */
public final class XMLDirectorySpoutSource implements ISpoutSource {

	private VTDGen vg = new VTDGen();
	private final String directory;
	private final Logger logger = LoggerFactory.getLogger(XMLDirectorySpoutSource.class);
	
	public XMLDirectorySpoutSource(final String directory) {
		this.directory = directory;
	}
	
	public List<TuplePair<VTDNav, String>> fetch() {
		List<TuplePair<VTDNav, String>> list = new ArrayList<>();
		try {
			logger.info("Starting fetching of source files");
			Path p = FileSystems.getDefault().getPath(directory);
			DirectoryStream<Path> ds = Files.newDirectoryStream(p, "*.xml");
			Iterator<Path> iter = ds.iterator();
			if(iter.hasNext()) {
				while(iter.hasNext()) {
					Path px = iter.next();
					if (vg.parseFile(px.toString(),true)) {
						list.add(new TuplePair<>(vg.getNav(), px.toAbsolutePath().toString()));
						Files.delete(px);
						vg.clear();
					}
				}
			}
			ds.close();
			logger.info("Done with fetching of source files");
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return list;
	}
	
	public void close() throws IOException {}
	
}
