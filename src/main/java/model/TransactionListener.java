package model;


@Component
public class TransactionListener {

    private final List<Transaction> receivedTransactions = new ArrayList<>();

    @KafkaListener(topics = "trader-updates", groupId = "midas-group")
    public void listen(Transaction transaction) {
        receivedTransactions.add(transaction);
    }

    public List<Transaction> getReceivedTransactions() {
        return receivedTransactions;
    }
}