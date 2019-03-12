package transport;

public class MainBatch {    
    public static void main(String[] args) throws Exception {
    	if(args ==null || args.length<=0){
    		 System.out.println("args is null");
    		 return;
    	}
    	Runner r =  new Runner(args[0],args[1]);
    	r.call();
    }
}
