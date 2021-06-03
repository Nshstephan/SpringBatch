package io.spring.batch.config;

import io.spring.batch.model.Employee;
import io.spring.batch.processor.DBLogProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Value("input/part*.csv")
    private Resource[] inputResources;

    /**
     * The multiResourceItemReader() method is used to read multiple CSV files
     */
    @Bean
    public MultiResourceItemReader<Employee> multiResourceItemReader()
    {
        MultiResourceItemReader<Employee> resourceItemReader = new MultiResourceItemReader<>();
        resourceItemReader.setResources(inputResources);
        resourceItemReader.setDelegate(reader());
        return resourceItemReader;
    }

    /**
     * The reader() method is used to read the data from the CSV file
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Bean
    public FlatFileItemReader<Employee> reader() {
        //Create reader instance
        FlatFileItemReader<Employee> reader = new FlatFileItemReader<Employee>();

        //Set number of lines to skips.
        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper() {
            {
                //3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[]{"firstName", "lastName", "date"});
                    }
                });
                //Set values in Employee class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {
                    {
                        setTargetType(Employee.class);
                    }
                });
            }
        });
        return reader;
    }

    /**
     * Intermediate processor to do the operations after the reading the data from the CSV file and
     * before writing the data into SQL.
     */
    @Bean
    public ItemProcessor<Employee, Employee> processor() {
        return new DBLogProcessor();
    }

    /**
     * The writer() method is used to write a data into the SQL.
     */
    @Bean
    public JdbcBatchItemWriter<Employee> writer()
    {
        JdbcBatchItemWriter<Employee> writer = new JdbcBatchItemWriter<Employee>();
        writer.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<Employee>());
        writer.setSql("INSERT INTO EMPLOYEE (FIRSTNAME, LASTNAME, DATE) VALUES (:firstName, :lastName, :date)");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Job insertEmployeeJob()
    {
        return jobBuilderFactory.get("importEmployeeJob").incrementer(new RunIdIncrementer())
                .start(step1()).build();
    }

    @Bean
    public Step step1()
    {
        return stepBuilderFactory.get("step1").<Employee, Employee>chunk(5)
                .reader(multiResourceItemReader())
                .processor(processor())
                .writer(writer()).build();
    }
}