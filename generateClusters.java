package finalProj.hadoop;

import java.io.Writer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class generateClusters {

	public static void main(String args[]) throws IOException {
		// Total number of points
		int pointCount = 100;
		// Range of randomness, 1....X
		int maxX = 1000;
		int maxY = 1000;

		buildPoints(pointCount, maxX, maxY);
	}

	public static void buildPoints(int pointCount, int maxX, int maxY) throws IOException {
		String filename = "points";
		// Ints for cluster number, scale for cluster, x/y centroid value, computed x/y offsets
		int xPos, yPos, cluster, scale = 50, xCentroid = 50, yCentroid = 50, xOffset, yOffset, px, py;

		// Number of clusters
		int clusterNum = 6;

		// create instance of Random class
		Random rand = new Random();
		String newline = System.getProperty("line.separator");

		// Generate and write points
		Writer writer = new BufferedWriter(new FileWriter(new File(filename)));
		for (int ic = 1; ic <= pointCount; ic++) {
			// Choose cluster
			cluster = rand.nextInt(clusterNum);
			
			// Get cluster info for new point
			switch (cluster) {
            			case 0: xCentroid = 800;
					yCentroid = 200;
					scale = 60;
                     			break;
            			case 1: xCentroid = 100;
					yCentroid = 100;
					scale = 40;
            		         	break;
            			case 2: xCentroid = 200;
					yCentroid = 150;
					scale = 20;
                     			break;
            			case 3: xCentroid = 400;
					yCentroid = 500;
					scale = 80;
                     			break;
            			case 4: xCentroid = 400;
					yCentroid = 500;
					scale = 100;
                     			break;
            			case 5: xCentroid = 500;
					yCentroid = 500;
					scale = 500;
                     			break;
        		}


			xPos = rand.nextInt(2);
			yPos = rand.nextInt(2);
			
			// Compute info for new point in relation to centroid
			xOffset = rand.nextInt(scale);
			yOffset = rand.nextInt(scale);

			if (xPos == 1) px = xCentroid + xOffset;
			else px = xCentroid - xOffset;
			if (yPos == 1) py = yCentroid + yOffset;
			else py = yCentroid - yOffset;

			// Write to file			
			if (ic > 1) {
				writer.write(newline);
			}

			writer.write(px + "," + py);
		}
		writer.close();
	}
}
