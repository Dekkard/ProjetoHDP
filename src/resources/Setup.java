package resources;

public class Setup {
	/*Variáveis de máximos*/
	public static String MAX_REQ = "max_requests";
	public static String MAX_SEC = "max_session";
	public static String MAX_GET = "max_get";
	public static String MAX_PUT = "max_put";
	public static String MAX_POST = "max_post";
	public static String MAX_DEL = "max_del";
	/*Total de requisições registradas na amostra N*/
	public static String N_TOTAL = "total"; //Integer
	/*Palavras que são escolhidas para as URLs serem aceitas ou rejeitadas*/
	public static String WORD_WANTED = "word_wanted";
	public static String WORD_UNWANTED = "word_unwanted";
	/*Tamanho do Cluster K*/
	public static String K_CLUSTER_SIZE = "K_CLUSTER_SIZE"; //Integer
	/*Quantidade de parâmetros D*/
	public static String D_PARAM_SIZE = "D_PARAM_SIZE"; //Integer
	/*Casas decimais*/
	public static String USCALE = "USCALE"; //Integer
	/*Margem de limite da comparação*/
	public static String ERROR_MARGIN = "ERROR_MARGIN"; // Double
	public static String MAX_ROUNDS = "MAX_ROUNDS";
	/*Caminhos do HDFS*/
	public static String INPUT_PATH = "INPUT_PATH";
	public static String OUTPUT_PATH = "OUTPUT_PATH";
	public static String JOB_PATH = "JOB_PATH";
	public static String JOB_PATH_KMEANS = "JOB_PATH_KMEANS";
	public static String JOB_PATH_KMEANS_ROUND = "JOB_PATH_KMEANS_ROUND";
	public static String CENTROID_CUR_PATH = "CENTROID_CUR_PATH";
	public static String CENTROID = "centroid";
	public static String NEW_CENTROID = "new_centroid";
	/*Outras variáveis*/
	public static String HIST_BIN_DIV = "HIST_BIN_DIV";
	public static String NORM_ALG = "NORM_ALG";
}
