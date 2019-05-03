import fileinput
import json
import random
import sys
 


if __name__ == '__main__':


    nclients = int(raw_input('Number of client : '))

    nresource = int(raw_input('Number of resourceProv : '))
    napplication = int(raw_input('Number of applicationDev : '))
    niterations = int(raw_input('Number of iterations : '))
    

    set_cost_developer = map(int,raw_input('Set of cost of developer : ').split(","))
    set_cost_r_provider = map(int,raw_input('Set of cost of resource provider : ').split(","))
    set_value_of_data = map(int,raw_input('Set value of data : ').split(","))
    
    probability_of_attacker_being_inspector = float(raw_input('Probability of attacker being inspector : '))
    probability_of_succesful_attack = float(raw_input('Probability of successful attack : '))

    trust_threshold = float(raw_input('Trust threshold : '))
    trust_to_external_factors = float(raw_input('Trust to external factors : '))
    trust_decrease_factors = float(raw_input('Trust decrease factor when attacker being inspector : '))


    print "\n<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>\n"

    '''
    print "\n"
    print (nclients)
    print (nresource)
    print (napplication)
    print (niterations)
    print (set_cost_developer)
    print (set_cost_r_provider)
    print (set_value_of_data)
    print (probability_of_attacker_being_inspector)
    print (probability_of_succesful_attack)
    print (trust_threshold)
    print (trust_to_external_factors)
    print (trust_decrease_factors)
    '''


    clients_perceptions_pairs_assigned={}
    
    external_trusts1=[1] * napplication
    external_trusts2=[1] * nresource
    external_trusts=[external_trusts1,external_trusts2]




    active_pairs={}
    for d_i in range(0, len(set_cost_developer)):
        for r_i in range(0, len(set_cost_r_provider)):
            active_pairs[(d_i,r_i)]=0


    for i in range(0, nclients):
        clients_trust_vector1=[10] * napplication
        clients_trust_vector2=[10] * nresource

        r1=random.randint(1,100)
        r2=random.randint(1,100)

        pairs=[r1%napplication,r2%nresource]

        active_pairs[(pairs[0],pairs[1])]=1

        clients_perceptions_pairs_assigned[i]=[clients_trust_vector1,clients_trust_vector2,pairs]





    filew=open("out.csv","w")


    #print "iteration,attacker,client1,client2,client3,res1,res2,res3,app1,app2,app3"
    printline= "iteration,attacker,client1,client2,client3,res1,res2,res3,app1,app2,app3"

    filew.write(printline+"\n")


    cumulative_client=[0]*nclients
    cumulative_res=[0]*nresource
    cumulative_app=[0]*napplication

    cumulative_attacker=0

    for i in range(0, niterations):

        for client in clients_perceptions_pairs_assigned:

            d_trust_vector=clients_perceptions_pairs_assigned.get(client)[0]
            r_trust_vector=clients_perceptions_pairs_assigned.get(client)[1]
            pairs=clients_perceptions_pairs_assigned.get(client)[2]

            ex_trust_d=external_trusts[0][pairs[0]]
            ex_trust_r=external_trusts[1][pairs[1]]

            trust_d=d_trust_vector[pairs[0]]
            trust_r=r_trust_vector[pairs[1]]

            oldpairs=pairs

            #####update trust perception
            new_d=trust_d-((10-ex_trust_d)*trust_to_external_factors)
            new_r=trust_r-((10-ex_trust_r)*trust_to_external_factors)
            if(new_d<trust_threshold): #pick new resource provider
                r1=random.randint(1,100)
                pairs[0]=r1%napplication    
                new_d=1                     #reset trust
            else:                           #commit to external_perception                        
                adam=1
            if(new_r<trust_threshold): #pick new app developer
                r2=random.randint(1,100)
                pairs[1]=r2%nresource
                new_r=1                     #reset trust
            else:                           #commit to external_perception
                adam=1
            

            active_pairs[(oldpairs[0],oldpairs[1])]=0
            active_pairs[(pairs[0],pairs[1])]=1

            ##


            
            d_trust_vector[pairs[0]]=new_d
            r_trust_vector[pairs[1]]=new_r
            clients_perceptions_pairs_assigned[client]=[d_trust_vector,r_trust_vector,pairs]


            utilitymax=-1*(sys.maxint-1)

            costr=-1
            costd=-1

            for d_i in range(0, len(set_cost_developer)):
                for r_i in range(0, len(set_cost_r_provider)):

                    utility1=(1-probability_of_succesful_attack)*(set_value_of_data[i%len(set_value_of_data)])
                    
                    c1=set_cost_developer[d_i%len(set_cost_developer)]
                    c2=set_cost_r_provider[r_i%len(set_cost_r_provider)]
                    
#                    c3=1.0/(c1*new_d)+1.0/(c2*new_r)

#                    utility=(utility1 - (c1+c2))/c3
                    utility=(utility1*(new_r+new_d))/(c1+c2)

                    if(utilitymax<utility):
                        utilitymax=utility
                        costd=c1
                        costr=c2
        
            cumulative_client[client]=cumulative_client[client]+utilitymax


        utility_attacker=0

        utility_app=[0] * napplication
        utility_res=[0] * nresource


        for active_pair in active_pairs:
            p1=-1
            p2=-1            

            if (active_pairs.get(active_pair)==1):
                p1=active_pair[0]
                p2=active_pair[1]
            else:
                continue

            v_d=set_value_of_data[i%len(set_value_of_data)]

            ex_trust_d=external_trusts[0][p1]
            ex_trust_r=external_trusts[1][p2]

            trust_T=(ex_trust_d+ex_trust_r)


            utilitymax=-1*(sys.maxint-1)

            costr=-1
            costd=-1


            for d_i in range(0, len(set_cost_developer)):
                for r_i in range(0, len(set_cost_r_provider)):

                    denom=(c1+c2)*trust_T

                    utility=((probability_of_succesful_attack*v_d)/denom)            

                    if(utilitymax<utility):
                        utilitymax=utility
                        costd=c1
                        costr=c2


            ins_att=random.uniform(0, 1)

            attack_p=random.uniform(0, 1)


            if(attack_p<probability_of_succesful_attack):
                
                utilityApp=0
                utilityRes=0
                utility_attacker=utility_attacker+utility
                if(ins_att<probability_of_attacker_being_inspector):
                    ##decrease external thing
                    
                    external_trusts1=external_trusts[0]
                    external_trusts2=external_trusts[1]
                    external_trusts1[p1]=external_trusts1[p1]-0.2
                    external_trusts2[p2]=external_trusts2[p2]-0.2
                    external_trusts[0]=external_trusts1
                    external_trusts[1]=external_trusts2

                    utilityApp=utilityApp-trust_decrease_factors
                    utilityRes=utilityRes-trust_decrease_factors

                else:

                    utilityApp=utilityApp+costd
                    utilityRes=utilityRes+costr

                
                utility_app[p1]=utility_app[p1]+utilityApp
                utility_res[p2]=utility_res[p2]+utilityRes

                cumulative_app[p1]=cumulative_app[p1]+utilityApp
                cumulative_res[p2]=cumulative_res[p2]+utilityRes
            
        cumulative_attacker=cumulative_attacker+utility_attacker


        printclient=""

        for c in cumulative_client:

            print c
            printclient=printclient+str(c)+","


        printres=""

        for c in cumulative_res:
            printres=printres+str(c)+","


        printapp=""

        for c in cumulative_app:
            printapp=printapp+str(c)+","




        #print str(i)+","+str(cumulative_attacker)+","+printclient+printres+printapp
        printline=str(i)+","+str(cumulative_attacker)+","+printclient+printres+printapp
        filew.write(printline+"\n")

    filew.close()






