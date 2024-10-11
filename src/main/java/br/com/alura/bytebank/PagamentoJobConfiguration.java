package br.com.alura.bytebank;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class PagamentoJobConfiguration {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    public Job job(Step importacao, JobRepository jobRepository) {
        return  new JobBuilder("importacao", jobRepository)
                .start(importacao)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step importacao(ItemReader<Pagamento> leitor, ItemWriter<Pagamento> escrita,
                           JobRepository jobRepository) {
        return  new StepBuilder("importacao-pagamento", jobRepository)
                .<Pagamento, Pagamento>chunk(10, transactionManager)
                .reader(leitor)
                .writer(escrita)
                .build();
    }

    @Bean
    public ItemReader<Pagamento> leitor() {
        return new FlatFileItemReaderBuilder<Pagamento>()
                .name("leitor")
                .resource(new FileSystemResource("files/dados_ficticios.csv"))
                .linesToSkip(1)
                .delimited()
                .delimiter("|")
                .names("funcionario", "cpf", "agencia", "conta", "valor", "mesReferencia")
                .targetType(Pagamento.class)
                .build();

    }

    @Bean
    public ItemWriter<Pagamento> escrita(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Pagamento>()
                .dataSource(dataSource)
                .sql(
                        "INSERT INTO pagamento (funcionario, cpf, agencia, conta, valor, mes_referencia)" +
                        " VALUES (:funcionario, :cpf, :agencia, :conta, :valor, :mesReferencia)"
                )
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
}
