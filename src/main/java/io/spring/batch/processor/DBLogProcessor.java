package io.spring.batch.processor;

import io.spring.batch.model.Employee;
import org.springframework.batch.item.ItemProcessor;

public class DBLogProcessor implements ItemProcessor<Employee, Employee>
{
    @Override
    public Employee process(final Employee employee)
    {
        System.out.println("Inserting employee : " + employee);
        return employee;
    }
}