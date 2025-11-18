import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class MongoMflixOperations {

    private static final String CONNECTION_STRING = "mongodb+srv://guipsn3_db_user:<db_password>@cluster0.9z1wp4n.mongodb.net/?appName=Cluster0"; 
    
    private static final String DATABASE_NAME = "sample_mflix";

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(CONNECTION_STRING)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> moviesCollection = database.getCollection("movies");
            MongoCollection<Document> commentsCollection = database.getCollection("comments");
            
            // Chame os métodos de operação aqui...
            // listMoviesByName(moviesCollection, "The Room");
            // listMoviesByRuntime(moviesCollection, 15);
            // list1980sMoviesByImdbRating(moviesCollection);
            // listMoviesByGenres(moviesCollection, Arrays.asList("Drama", "Comedy"));
            // listAwardedMovies(moviesCollection, 3);
            // insertMovieTriumphOfNerds(moviesCollection);
            // insertMovieSiliconCowboys(moviesCollection, commentsCollection);
            // updateOldMovies(moviesCollection, 1950);
            
            // Para a parte CRUD:
            // runPessoaCrudExample(mongoClient.getDatabase("meusdados"));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void listMoviesByName(MongoCollection<Document> collection, String movieTitle) {
        System.out.println("--- 1. Filmes com nome \"" + movieTitle + "\" ---");
        
        // Query: { "title": "The Room" }
        collection.find(Filters.eq("title", movieTitle))
                  .forEach(doc -> System.out.println("Título: " + doc.getString("title") + ", Ano: " + doc.getInteger("year")));
    }

    public static void listMoviesByRuntime(MongoCollection<Document> collection, int maxRuntime) {
        System.out.println("\n--- 2. Filmes com duração <= " + maxRuntime + " minutos ---");
        
        // Query: { "runtime": { "$lte": 15 } }
        collection.find(Filters.lte("runtime", maxRuntime))
                  .projection(Projections.include("title", "runtime"))
                  .forEach(doc -> System.out.println("Título: " + doc.getString("title") + ", Duração: " + doc.getInteger("runtime") + " min"));
    }

    public static void list1980sMoviesByImdbRating(MongoCollection<Document> collection) {
        System.out.println("\n--- 3. Filmes dos anos 80, ordenados por IMDB Rating ---");
        
        // Query: { "year": { "$gte": 1980, "$lte": 1989 } }
        Document query = Filters.and(
            Filters.gte("year", 1980),
            Filters.lte("year", 1989)
        );
        
        // Sort: { "imdb.rating": -1 }
        collection.find(query)
                  .sort(Sorts.descending("imdb.rating"))
                  .limit(10) // Limitando a 10 para exemplo
                  .projection(Projections.include("title", "year", "imdb.rating"))
                  .forEach(doc -> {
                      Document imdb = doc.get("imdb", Document.class);
                      System.out.println("Título: " + doc.getString("title") + 
                                         ", Ano: " + doc.getInteger("year") +
                                         ", Rating: " + (imdb != null ? imdb.getDouble("rating") : "N/A"));
                  });
    }

    public static void listMoviesByGenres(MongoCollection<Document> collection, List<String> requiredGenres) {
        System.out.println("\n--- 4. Filmes que são \"Drama\" E \"Comedy\" ---");
        
        // Query: { "genres": { "$all": ["Drama", "Comedy"] } }
        collection.find(Filters.all("genres", requiredGenres))
                  .projection(Projections.include("title", "genres"))
                  .limit(10)
                  .forEach(doc -> System.out.println("Título: " + doc.getString("title") + ", Gêneros: " + doc.getList("genres", String.class)));
    }

    public static void listAwardedMovies(MongoCollection<Document> collection, int minWins) {
        System.out.println("\n--- 5. Filmes com mais de " + minWins + " prêmios, ordenados alfabeticamente ---");
        
        // Query: { "awards.wins": { "$gt": 3 } }
        collection.find(Filters.gt("awards.wins", minWins))
                  .sort(Sorts.ascending("title"))
                  .projection(Projections.include("title", "awards.wins"))
                  .limit(10)
                  .forEach(doc -> {
                      Document awards = doc.get("awards", Document.class);
                      System.out.println("Título: " + doc.getString("title") + 
                                         ", Prêmios Ganhos: " + (awards != null ? awards.getInteger("wins") : 0));
                  });
    }

    public static void insertMovieTriumphOfNerds(MongoCollection<Document> collection) {
        System.out.println("\n--- 6. Inserir o filme \"O Triunfo dos Nerds\" ---");
        
        Document movieDoc = new Document("_id", new ObjectId())
                .append("title", "O Triunfo dos Nerds")
                .append("year", 1996)
                .append("runtime", 180)
                .append("plot", "Documentário sobre a história dos computadores pessoais.")
                .append("genres", Arrays.asList("Documentary", "History"));
    
        collection.insertOne(movieDoc);
        System.out.println("Filme inserido com sucesso! ID: " + movieDoc.getObjectId("_id"));
    }

    public static void insertMovieSiliconCowboys(MongoCollection<Document> moviesCollection, MongoCollection<Document> commentsCollection) {
        System.out.println("\n--- 7. Inserir \"Silicon Cowboys\" e dois comentários ---");
    
        // 7.1. Inserir o Filme
        ObjectId movieId = new ObjectId();
        Document movieDoc = new Document("_id", movieId)
                .append("title", "Silicon Cowboys")
                .append("year", 2016)
                .append("plot", "A história da Compaq, que desafiou a IBM.")
                .append("genres", Arrays.asList("Documentary"));
                
        moviesCollection.insertOne(movieDoc);
        System.out.println("Filme inserido com sucesso! ID: " + movieId);
    
        // 7.2. Inserir os Comentários (referenciando o ID do filme)
        Document comment1 = new Document("_id", new ObjectId())
                .append("movie_id", movieId) // Chave estrangeira no modelo relacional
                .append("text", "Um ótimo documentário sobre a história da tecnologia.")
                .append("name", "Alice");
    
        Document comment2 = new Document("_id", new ObjectId())
                .append("movie_id", movieId)
                .append("text", "A rivalidade Compaq x IBM é fascinante!")
                .append("name", "Bob");
    
        commentsCollection.insertMany(Arrays.asList(comment1, comment2));
        System.out.println("Dois comentários inseridos na coleção 'comments' para o filme " + movieId);
    }

    public static void updateOldMovies(MongoCollection<Document> collection, int maxYear) {
        System.out.println("\n--- 8. Atualizar filmes antes de " + maxYear + " com o gênero 'old' ---");
        
        // Query: { "year": { "$lt": 1950 } }
        Document query = Filters.lt("year", maxYear);
    
        // Update: { "$addToSet": { "genres": "old" } }
        // O $addToSet garante que "old" só será adicionado uma vez.
        Document update = Updates.addToSet("genres", "old");
    
        // Realiza a atualização para todos os documentos que correspondem ao filtro
        long modifiedCount = collection.updateMany(query, update).getModifiedCount();
    
        System.out.println(modifiedCount + " filmes atualizados. Gênero 'old' adicionado para filmes anteriores a " + maxYear + ".");
    }

    import org.bson.types.ObjectId;

// Subclasse para o Endereço (atributo composto)
class Endereco {
    private String rua;
    private String bairro;
    private String cep;
    private String tipo;

    // Construtor, Getters e Setters... (Omitidos por brevidade)
    public Endereco(String rua, String bairro, String cep, String tipo) {
        this.rua = rua; this.bairro = bairro; this.cep = cep; this.tipo = tipo;
    }
    
    // Método para converter o objeto Endereco em um Document do MongoDB
    public Document toDocument() {
        return new Document()
                .append("rua", rua)
                .append("bairro", bairro)
                .append("cep", cep)
                .append("tipo", tipo);
    }
}

// Classe Principal
class Pessoa {
    private ObjectId id; // Usamos ObjectId para o _id do MongoDB
    private String nome;
    private String telefone;
    private Endereco endereco; // Atributo composto

    // Construtor, Getters e Setters... (Omitidos por brevidade)
    public Pessoa(String nome, String telefone, Endereco endereco) {
        this.nome = nome; this.telefone = telefone; this.endereco = endereco;
        this.id = new ObjectId(); // Gera um novo ID ao criar a Pessoa
    }
    
    public ObjectId getId() { return id; }

    // Método para converter o objeto Pessoa em um Document do MongoDB
    public Document toDocument() {
        return new Document("_id", id)
                .append("nome", nome)
                .append("telefone", telefone)
                .append("endereco", endereco.toDocument()); // Subdocumento
    }
}

class PessoaDAO {
    private final MongoCollection<Document> collection;

    public PessoaDAO(MongoDatabase database) {
        // Usa uma nova coleção para os dados de Pessoa
        this.collection = database.getCollection("pessoas");
    }

    // 1. CREATE (C)
    public void create(Pessoa pessoa) {
        collection.insertOne(pessoa.toDocument());
        System.out.println("Pessoa criada: " + pessoa.getNome() + " com ID: " + pessoa.getId());
    }

    // 2. READ (R) - Busca por ID
    public Pessoa findById(ObjectId id) {
        Document doc = collection.find(Filters.eq("_id", id)).first();
        if (doc == null) {
            return null;
        }
        
        // Simplesmente imprime para demonstração
        System.out.println("Pessoa encontrada: " + doc.toJson());
        return null; // Retornaria o objeto Pessoa reconstituído se fosse um sistema real
    }

    // 3. UPDATE (U) - Atualiza o telefone
    public long update(ObjectId id, String novoTelefone) {
        // Query: { "_id": id }
        Document query = Filters.eq("_id", id);
        
        // Update: { "$set": { "telefone": novoTelefone } }
        Document update = Updates.set("telefone", novoTelefone);
        
        long count = collection.updateOne(query, update).getModifiedCount();
        System.out.println(count + " pessoa(s) atualizada(s).");
        return count;
    }

    // 4. DELETE (D)
    public long delete(ObjectId id) {
        // Query: { "_id": id }
        long count = collection.deleteOne(Filters.eq("_id", id)).getDeletedCount();
        System.out.println(count + " pessoa(s) excluída(s).");
        return count;
    }
}

public static void runPessoaCrudExample(MongoDatabase database) {
    System.out.println("\n--- 9. Exemplo CRUD (PessoaDAO) ---");
    PessoaDAO pessoaDAO = new PessoaDAO(database);

    // C - CREATE
    Endereco end = new Endereco("Rua Java", "Centro", "12345-000", "Residencial");
    Pessoa novaPessoa = new Pessoa("Carlos Java", "9999-8888", end);
    pessoaDAO.create(novaPessoa);
    ObjectId pessoaId = novaPessoa.getId();

    // R - READ
    pessoaDAO.findById(pessoaId);

    // U - UPDATE
    pessoaDAO.update(pessoaId, "9876-5432");
    
    // R - READ (novamente para ver a mudança)
    pessoaDAO.findById(pessoaId);

    // D - DELETE
    pessoaDAO.delete(pessoaId);
    
    // R - READ (tentar encontrar após exclusão)
    System.out.print("Tentativa de encontrar após exclusão: ");
    pessoaDAO.findById(pessoaId);
}
}
