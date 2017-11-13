package com.productiveAnalytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Streamer {
	private static final int MAX_NUM = 1_000;
	private static final int DIVISOR = 17;
	
	private static long startTime, endTime;
	
	public static void main (String[] args)
	{
		List<Integer> myList = new ArrayList<Integer>();
		for(int i=1; i<=MAX_NUM; ++i) myList.add(i);
		
		startTime = System.currentTimeMillis();
		performSerially(myList);
		endTime = System.currentTimeMillis();
		final long serialOpDuration = (endTime-startTime);
		System.out.println("Serial Operation took "+ serialOpDuration +"ms");
		
		System.out.println("\n");
		
		startTime = System.currentTimeMillis();
		performParallely(myList);
		endTime = System.currentTimeMillis();
		final long parallelOpDuration = (endTime-startTime);
		System.out.println("Parallel Operation took "+ parallelOpDuration +"ms");
		
		System.out.println("\n>>> Parallel steaming is "+ Math.floor((serialOpDuration-parallelOpDuration)/parallelOpDuration) +" times faster that Serial streaming.");
		
		mapReduce(myList);
		
		List<Integer> intVals1 = Arrays.asList(1, 2, 3, 5, 7, 8, 10, 12, 13, 15, 16, 18);
		List<Integer> intVals2 = Arrays.asList(0, 2, 9, 11, 15, 17, 19, 21, 23, 27, 29, 31, 37);
		
		Integer retVal;
		
		System.out.println("--- Traditiona Looping (eager evaluation) ---");
		retVal = traditionalImperativeLoop(intVals1, 10);
		System.out.println("Traditional loop # 1: Return val = "+ retVal);
		System.out.println("~~~");
		retVal =traditionalImperativeLoop(intVals2, 15);
		System.out.println("Traditional loop # 2: Return val = "+ retVal);
		System.out.println("--- Traditiona Looping (eager evaluation) Ends ---");
		
		System.out.println("\n\n*** Functional Programming (Lazy evaluation) ***");
		retVal = functionalProgrammingLambda(intVals1, 10);
		System.out.println("Functional lambda # 1: Return val = "+ retVal);
		System.out.println("~~~");
		retVal = functionalProgrammingLambda(intVals2, 15);
		System.out.println("Functional lambda # 2: Return val = "+ retVal);
		System.out.println("*** Functional Programming (Lazy evaluation) Ends ***");
	}
	
	private static void performSerially (final List<Integer> myList)
	{
		// Sequential Stream
		final Stream<Integer> sequentialStream = myList.stream();
		Stream<Integer> seqResult = sequentialStream.filter(s -> s % DIVISOR == 0);
		seqResult.forEach(s -> System.out.println("Sequential: "+ s));
		
		// Calling the operation on already processed Stram will give Exception,
		// as the Stream cannot be reused
//		seqResult.forEach(s -> System.out.println("Sequential: "+ s));
		Stream<Integer> seqResult2 = myList.stream().filter(s -> s % DIVISOR == 0);
		int serialResult = seqResult2.reduce(0, (cumul , elem) -> cumul + elem);
		System.out.println("Serial TOTAL = "+ serialResult);
	}
	
	private static void performParallely (final List<Integer> myList)
	{
		// Parallel Stream (may be out-of-order)
		final Stream<Integer> parallelStream = myList.parallelStream();
		Stream<Integer> parlResult = parallelStream.filter(p -> p % DIVISOR == 0);
		parlResult.forEach(p -> System.out.println("Parallel: "+ p));
	}
	
	private static void mapReduce (final List<Integer> myList)
	{
		Optional<Integer> opt = myList.parallelStream().filter(s -> s % DIVISOR == 0)
													   .map(e -> e*2)
													   .reduce((c,e) -> Integer.sum(c, e));
		System.out.println("\n\nMap-Reduce : SUM (e*2) = "+ (Integer)opt.get());
	}
	
	private static boolean isEven(int num) {
		System.out.println("Chekcing if "+ num +" is even.");
		return num % 2 == 0;
	}
	
	private static boolean isGreaterThan(int num, int pivot) {
		System.out.println("Comparing if "+ num +" > "+ pivot);
		return num > pivot;
	}
	
	private static int doubleIt(int num) {
		System.out.println("Doubling..."+ num );
		return num * 2;
	}
	
	private static Integer traditionalImperativeLoop (final List<Integer> intList, final int pivot)
	{
		startTime = System.currentTimeMillis();
		Integer retValue = null; 
		
		// find first that is greater than given pivot
		// and is even number
		// and return it's double (e * 2) 
		for (int elem : intList)
		{
			if (isGreaterThan(elem, pivot) && isEven(elem))
			{
				retValue = doubleIt(elem);
				break;
			}
		}
		
		endTime = System.currentTimeMillis();
		
		System.out.println("Traditional looping took "+ (endTime-startTime) +"ms");
		return retValue;
	}
	
	private static Integer functionalProgrammingLambda(final List<Integer> intList, final int pivot)
	{
		startTime = System.currentTimeMillis();
		Integer retValue = null;
		
		Stream<Integer> intStream = intList.stream();
		intStream.filter(e -> isGreaterThan(e, pivot))
				 .filter(Streamer :: isEven)
				 .map(Streamer :: doubleIt)
				 .findFirst();
		
		endTime = System.currentTimeMillis();
		
		System.out.println("Functional Lambda took "+ (endTime-startTime) +"ms");
		return retValue;
	}
}
