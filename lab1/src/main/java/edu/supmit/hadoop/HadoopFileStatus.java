package edu.supmit.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: HadoopFileStatus <chemin> <fichier> <nouveau_nom>");
            System.err.println("Exemple: HadoopFileStatus /user/root/input purchases.txt achats.txt");
            System.exit(1);
        }
        
        String chemin = args[0];     
        String fichier = args[1];    
        String nouveauNom = args[2];  
        
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
           Path filepath = new Path(chemin, fichier); 
            FileStatus infos = fs.getFileStatus(filepath);
            
            if(!fs.exists(filepath)){
                System.out.println("File does not exists");
                System.exit(1);
            }
            
            System.out.println(Long.toString(infos.getLen())+" bytes");
            System.out.println("File Name: "+filepath.getName());
            System.out.println("File Size: "+infos.getLen());          
            System.out.println("File owner: "+infos.getOwner());       
            System.out.println("File permission: "+infos.getPermission()); 
            System.out.println("File Replication: "+infos.getReplication());
            System.out.println("File Block Size: "+infos.getBlockSize());
            BlockLocation[] blockLocations = fs.getFileBlockLocations(infos, 0, infos.getLen());
            for(BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            
            // Renommer le fichier
            fs.rename(filepath, new Path(chemin, nouveauNom));
           System.out.println("File renamed successfully from " + fichier + " to " + nouveauNom);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}