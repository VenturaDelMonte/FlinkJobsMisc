package de.adrianbartnik.job.stateful;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParallelSocketOperatorStateJobTest {

    @Test
    public void isPowerMachineNumber() {
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("1"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("2"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("3"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("4"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("5"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("6"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("7"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("8"));
        assertTrue(ParallelSocketOperatorStateJob.IsPowerMachineNumber("9"));
        assertFalse(ParallelSocketOperatorStateJob.IsPowerMachineNumber("0"));
        assertFalse(ParallelSocketOperatorStateJob.IsPowerMachineNumber("10"));
    }

    @Test
    public void isSingleDigit() {
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("0"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("1"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("2"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("3"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("4"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("5"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("6"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("7"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("8"));
        assertTrue(ParallelSocketOperatorStateJob.IsSingleDigit("9"));
        assertFalse(ParallelSocketOperatorStateJob.IsSingleDigit("10"));
    }
}