%***************************************************************************************************

1 ;  # Prevent Octave from thinking that this is a function file

%***************************************************************************************************

more  off ;
clear all ;
clc       ;

%***************************************************************************************************

global h =    10    ;  % Image height
global w =    10    ;  % Image width
global p =   10    ;  % Population size
global r =    0.90 ;  % Mutation probability
global t =    0.95 ;  % Acceptable rate of fitness
global f = 9999    ;  % Maximum populations			

%---------------------------------------------------------------------------------------------------

global s = h * w   ;  % Image size in pixels
global m = s       ;  % Maximum possible fitness value

%***************************************************************************************************

% image  : an h x w rectangular matrix
% vector : a column vector of size s

function vector = imageToVector ( image )

  global s ;

  vector = reshape( image , s , 1 ) ;

endfunction

%***************************************************************************************************

% vector : a column vector of size s
% image  : an h x w rectangular matrix

function image = vectorToImage ( vector )

  global h w ;

  image = reshape( vector , h , w ) ;

endfunction

%***************************************************************************************************

% cumulativeDistribution : a row vector of size p with cumulative probabilities (last entry is 1.0)
% index                  : index, selected using fitness proportionate (roulette wheel) selection

function index = rouletteWheelSelect ( cumulativeDistribution )

  global p ;

  index = 1      ;
  value = rand() ;

  for i = 1 : p

    if ( cumulativeDistribution( 1 , i ) >= value )

      index = i ;
      break     ;

    endif

  endfor

endfunction

%***************************************************************************************************

% Performs single-point crossover on two column vectors (parents)
% Result is two column vectors (offsprings)

function [ child1 , child2 ] = crossover ( parent1 , parent2 )

  global s ;

  splitPoint = randi( s - 1 ) ;

  child1(1            : splitPoint , 1) = parent1(1            : splitPoint , 1) ;
  child1(splitPoint+1 : s          , 1) = parent2(splitPoint+1 : s          , 1) ;

  child2(1            : splitPoint , 1) = parent2(1            : splitPoint , 1) ;
  child2(splitPoint+1 : s          , 1) = parent1(splitPoint+1 : s          , 1) ;

endfunction

%***************************************************************************************************

% Performs two-point crossover on two column vectors (parents)
% Result is two column vectors (offsprings)

function [ child1 , child2 ] = crossover2 ( parent1 , parent2 )

  global s ;

  splitPoint1 =               randi( s - 2           ) ;
  splitPoint2 = splitPoint1 + randi( s - splitPoint1 ) ;

  child1( : , 1 ) = parent1( : , 1 ) ;
  child2( : , 1 ) = parent2( : , 1 ) ;

  child1( splitPoint1 : splitPoint2 , 1 ) = parent2( splitPoint1 : splitPoint2 , 1 ) ;
  child2( splitPoint1 : splitPoint2 , 1 ) = parent1( splitPoint1 : splitPoint2 , 1 ) ;

endfunction

%***************************************************************************************************

% vector : a column vector of size s
% Result is another column vector of size s with one randomly mutated entry

function mutated = mutate ( vector )

  global s ;

  mutationPoint = randi( s ) ;

  mutated( :             , 1 ) = vector( : , 1 ) ;
  mutated( mutationPoint , 1 ) = rand()          ;

endfunction

%***************************************************************************************************

% vector : a column vector of size s
% value  : fitness score between 0 and m

function value = fitness ( vector )

  global h w s ;

  f = 0 ;
  for p = 0 : (s-1)
    f += abs(vector(p+1,1)-(sin((sqrt((1+floor(p/h)-(w/2))^2+(-(1+mod(p,h)-(h/2)))^2)  / ...
         sqrt((w/2)^2+(h/2)^2))^0.6*25.13274122872)+10*(abs(atan2((-(1+mod(p,h)-(h/2))), ...
         (1+floor(p/h)-(w/2)))*57.295779513)/180))/11) ;
  endfor
  value = (s-f) ;

endfunction

%***************************************************************************************************

% populationSize        : number of individuals in the population
% mutationProbability   : probability of applying mutation to each offspring
% acceptableFitnessRate : stopping criterion (if fitness reaches this rate * m, training can stop)
% maximumPopulations    : stopping criterion (if iteration count exceeds this, training can stop)
% bestIndividual        : a column vector of size s
% fitnessHistory        : a row vector that keeps maximum fitness of each population in training

function [ bestIndividual , fitnessHistory ] = geneticAlgorithm ( populationSize        , ...
                                                                  mutationProbability   , ...
                                                                  acceptableFitnessRate , ...
                                                                  maximumPopulations    )
  global h w s m ;


  populationNo = 1                          ;
  population   = rand( s , populationSize ) ;  % Initial population


############################################	

	global r p ;


	while populationNo < maximumPopulations

		fitnessValues=[];
		fittest=-1;
		sumFitness=0;
		probabilities=[];	
		cumulative=[];
	
		for index = 1:populationSize			
			individual = population(:,index);
			fitnessValues(end+1)=fitness(individual);		#fitness value leri arraya atyom 
			sumFitness=sumFitness+fitness(individual);		#fitness value leri toplyom ilerde probability hesaplicam
			if(fittest<fitness(individual))
				fittest=fitness(individual);
				localbestIndividual=individual;
			end
		end	
		fitnessHistory(end+1)=fittest;
	
		if(populationNo==1)
			bestIndividual=localbestIndividual;
		end
	
		if(fitness(bestIndividual)<fitness(localbestIndividual))
			bestIndividual=localbestIndividual;
		end

		if(fitness(bestIndividual)>m*acceptableFitnessRate)
			break;	
		end

		sumProb=0;
		for index = 1:populationSize
			probabilities(end+1)=fitnessValues(:,index)/sumFitness;
			sumProb=sumProb+fitnessValues(:,index)/sumFitness;
			cumulative(end+1)=sumProb;
		end

##############################################

	
		newpopulation=[];
		
		fromprev=0;
		while fromprev<((1-r)*p)
			index=rouletteWheelSelect(cumulative);		
			newpopulation(:,fromprev+1)=population(:,index);								
			fromprev++;
		end

		forCross=0;
		while forCross<((r*p)/2)
		
			index1=rouletteWheelSelect(cumulative);
			index2=rouletteWheelSelect(cumulative);
		
			[offspring1,offspring2]=crossover(population(:,index1),population(:,index2));
			if (mod(forCross,2))
				[offspring1,offspring2]=crossover2(population(:,index1),population(:,index2));
			end	


			if(fromprev<p)		
				newpopulation(:,fromprev+1)=offspring1;
				fromprev++;
			end

			if(fromprev<p)
				newpopulation(:,fromprev+1)=offspring2;
				fromprev++;
			end
			forCross++;
		end


		mym=rand();
		counter=0;
		while counter<(mym*p)
			counter++;
			index=rouletteWheelSelect(cumulative);
			mutated = mutate ( newpopulation(:,index) );
			newpopulation(:,index)=mutated;
		end

		population=newpopulation;
	#	display(populationNo);
		populationNo = populationNo + 1;
	end



endfunction

%***************************************************************************************************

% Main part of the script

figure(1) ;  clf ;

[ bestIndividual , fitnessHistory ] = geneticAlgorithm( p , r , t , f ) ;

bestImage = vectorToImage( bestIndividual ) ;

imshow ( bestImage                ) ;
imwrite( bestImage , "Result.jpg" ) ;

figure(2) ;  clf ;

plot  ( fitnessHistory , "r" ) ;  grid on ;
xlabel( "Populations"        ) ;
ylabel( "Maximum Fitness"    ) ;

print -djpg "Fitness.jpg" ;

%***************************************************************************************************

