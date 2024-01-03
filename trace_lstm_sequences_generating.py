#!/usr/bin/env python
# coding: utf-8

# In[1]:


# https://pytorch.org/tutorials/beginner/nlp/sequence_models_tutorial.html
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import os

torch.manual_seed(1)


# In[2]:


# lele construct the training datasets


def prepare_sequence(seq, to_ix):
    idxs = [to_ix[w] for w in seq]
    return torch.tensor(idxs, dtype=torch.long)


# ix_to_tag = {v:k for k,v in tag_to_ix.items()}
# definition
id_to_nodes = {"Af": "FileScan FactTable", "Ad": "FileScan DimTable", "B": "BroadcastHashJoin", "C": "SortMergeJoin",
               "D": "Union", "E": ""}
id_to_operators = {"": "", "1": "Filter", "2": "Project", "3": "HashAggregate", "4": "Sort",
                   "5": "TakeOrderedAndProject", "6": "Expand", "7": "Window", "8": "SortAggregate"}


# These will usually be more like 32 or 64 dimensional.
# We will keep them small, so we can see how the weights change as we train.
EMBEDDING_DIM = 6
HIDDEN_DIM = 6
EPOCHES = 500


training_data = [
    ("AfB BE".split(), ["1", "2-4-7-4-7-4-7-4-7-4-7-4-7"]),  # app1-a-plan
    #     ("AfB BE".split(), ["1-2", "2-4-7-4-7-4-7-4-7-4-7-4-7"]),  # app1-a-plan

    ("AfE".split(), ["1-2-4-8-4-8-8-4-8"]),  # app1-b-plan

    #     ("AfE".split(), ["1-2-4-8-4-8-8-4-8"]),  # app1-query28

    #     ("AfC CC CC CC CC CE".split(), ["4", "2", "2", "2", "2", "2"]),  # app1-query30
    # #     ("AfC CC CC CC CC CE".split(), ["1-4", "2", "2", "2", "2", "2"]),  # app1-query30
    #     ("AfC CC CC CC CE".split(), ["1-4", "2", "2", "2", "2"]),  # app1-query30
    #     ("AfC CC CC CE".split(), ["1-4", "2", "2", "2"]),  # app1-query30
    #     ("AfC CC CE".split(), ["1-4", "2", "2"]),  # app1-query30
    #     ("AfC CE".split(), ["1-4", "2"]),  # app1-query30

    ("AfE".split(), ["1-2"]),  # app3-a-plan

    #     ("AfE".split(), ["1-2"]),  # app5-a-plan

    ("AfD DB BB BE".split(), ["1-2", "", "2", "2"]),  # app5-b-plan
    #     ("AfD DB BB BE".split(), ["1-2", "", "2", "2"]),  # app5-b-plan
    ("AfB BE".split(), ["1", "2"]),  # app5-b-plan
    ("AfB BD DC CE".split(), ["1-2-1-2", "1", "4-8-4-8-4", "2-3-3"]),  # app6-a-plan
    ("AfD DC CE".split(), ["1-2", "4-8-4-8-4", "2-3-3"]),  # app6-a-plan
    ("AfC CE".split(), ["1-2", "2-3-3"]),  # app6-a-plan

    ("AfD DE".split(), ["1-2-6-3-3-4-7-1-2", ""]),  # app8-a-plan
    #     ("AfD DE".split(), ["1-2-6-3-3-4-7-1-2", ""]),  # app8-a-plan
    #     ("AfD DE".split(), ["1-2-6-3-3-4-7-1-2", ""]),  # app8-a-plan

    ("AfC CE".split(), ["1-2-3-4", "2"]),  # app10-a-plan
    ("AfC CC CE".split(), ["1-2-4-8-4-8-1-2-4", "2", "2"]),  # app10-a-plan
    ("AfC CC CE".split(), ["1-2-4-8-4-8", "", "2"]),  # app10-a-plan

    ("AfB BC CC CE".split(), ["1-2-3", "2-3", "2-3-4", "2-4-8-4-8"]),  # app11-a-plan
    #     ("AfB BC CC CE".split(), ["1-2-3", "2-3", "2-3-4", "2-4-8-4-8"]),  # app11-a-plan
    ("AfC CC CE".split(), ["1-2-3-4", "2-3-4", "2-4-8-4-8"]),  # app11-a-plan
    ("AfC CE".split(), ["1-2-4", "2-4-8-4-8"]),  # app11-a-plan

    ("AfE".split(), ["2"]),  # app23-a-plan
    #     ("AfE".split(), ["2"]),  # app25-a-plan
    ("AfE".split(), ["1-2"]),  # app26-a-plan

]

word_to_ix = {}
# For each words-list (sentence) and tags-list in each tuple of training_data
for sent, tags in training_data:
    for word in sent:
        if word not in word_to_ix:  # word has not been assigned an index yet
            word_to_ix[word] = len(word_to_ix)  # Assign each word with a unique index
# print(word_to_ix)

tag_to_ix = {}
for sent, tags in training_data:
    for tag in tags:
        if tag not in tag_to_ix:  # tag has not been assigned an index yet
            tag_to_ix[tag] = len(tag_to_ix)  # Assign each tag with a unique index
# print(tag_to_ix)

ix_to_tag = {v: k for k, v in tag_to_ix.items()}
# print(ix_to_tag)

# tag_to_ix = {"DET": 0, "NN": 1, "V": 2}  # Assign each tag with a unique index



# In[3]:


class LSTMTagger(nn.Module):

    def __init__(self, embedding_dim, hidden_dim, vocab_size, tagset_size):
        super(LSTMTagger, self).__init__()
        self.hidden_dim = hidden_dim

        self.word_embeddings = nn.Embedding(vocab_size, embedding_dim)

        # The LSTM takes word embeddings as inputs, and outputs hidden states
        # with dimensionality hidden_dim.
        self.lstm = nn.LSTM(embedding_dim, hidden_dim)

        # The linear layer that maps from hidden state space to tag space
        self.hidden2tag = nn.Linear(hidden_dim, tagset_size)

    def forward(self, sentence):
        embeds = self.word_embeddings(sentence)
        lstm_out, _ = self.lstm(embeds.view(len(sentence), 1, -1))
        tag_space = self.hidden2tag(lstm_out.view(len(sentence), -1))
        tag_scores = F.log_softmax(tag_space, dim=1)
        return tag_scores


# In[4]:

class LSTMWrapper:
    def __init__(self):
        self.model = LSTMTagger(EMBEDDING_DIM, HIDDEN_DIM, len(word_to_ix), len(tag_to_ix))
        self.loss_function = nn.NLLLoss()
        self.optimizer = optim.SGD(self.model.parameters(), lr=0.1)

    def train(self):
        # See what the scores are before training
        # Note that element i,j of the output is the score for tag j for word i.
        # Here we don't need to train, so the code is wrapped in torch.no_grad()
        with torch.no_grad():
            inputs = prepare_sequence(training_data[0][0], word_to_ix)
            tag_scores = self.model(inputs)
            # print(tag_scores)

        for epoch in range(EPOCHES):  # again, normally you would NOT do 300 epochs, it is toy data
            for sentence, tags in training_data:
                # Step 1. Remember that Pytorch accumulates gradients.
                # We need to clear them out before each instance
                self.model.zero_grad()

                # Step 2. Get our inputs ready for the network, that is, turn them into
                # Tensors of word indices.
                sentence_in = prepare_sequence(sentence, word_to_ix)
                targets = prepare_sequence(tags, tag_to_ix)

                # Step 3. Run our forward pass.
                tag_scores = self.model(sentence_in)

                # print("sentence_in.shape is {}".format(sentence_in.shape))
                # print("tag_scores.shape is {}".format(tag_scores.shape))

                # print("targets.shape is {}".format(targets.shape))

                # Step 4. Compute the loss, gradients, and update the parameters by
                #  calling optimizer.step()
                loss = self.loss_function(tag_scores, targets)
                loss.backward()
                self.optimizer.step()

        # See what the scores are after training
        # with torch.no_grad():
        #     inputs = prepare_sequence(training_data[0][0], word_to_ix)
        #     tag_scores = self.model(inputs)
        #
        #     # The sentence is "the dog ate the apple".  i,j corresponds to score for tag j
        #     # for word i. The predicted tag is the maximum scoring tag.
        #     # Here, we can see the predicted sequence below is 0 1 2 0 1
        #     # since 0 is index of the maximum value of row 1,
        #     # 1 is the index of maximum value of row 2, etc.
        #     # Which is DET NOUN VERB DET NOUN, the correct sequence!
        #     # {'1': 0, '2-4': 1, '2': 2, '2-3-3-1-5': 3, '2-3-3-5': 4}
        #     print(tag_scores)
        #     print(torch.max(tag_scores, 1))
        #
        # # In[5]:

        correct_predictions = 0
        total_predictions = 0
        for sentence, tags in training_data:
            # Prepare the sentence for the model
            sentence_in = prepare_sequence(sentence, word_to_ix)

            # Get predicted tag indices
            predicted_tags = torch.argmax(self.model(sentence_in), dim=1)

            # Convert tensor to numpy array
            predicted_tags = predicted_tags.numpy()

            # Convert ground truth tags to indices
            true_tags = [tag_to_ix[tag] for tag in tags]

            # Calculate accuracy for this sentence
            correct_predictions += sum(
                predicted_tag == true_tag for predicted_tag, true_tag in zip(predicted_tags, true_tags))
            total_predictions += len(tags)

        accuracy = correct_predictions / total_predictions
        print("Accuracy:", accuracy)

        ##模型保存
        torch.save(self.model, "./trace_lstm_predictor.pt")
        print("save model successfully")

    def predict(self, long_chain):
        ##模型加载
        print(os.getcwd())
        load_model = torch.load("./trace_lstm_predictor.pt")
        ##设置模型进行测试模式
        load_model.eval()
        test_seq = long_chain.split()
        # test_seq="CB BE".split()
        inputs = prepare_sequence(test_seq, word_to_ix)
        tag_scores = load_model(inputs)
        # print(tag_scores)

        ans = torch.max(tag_scores, 1)
        ans_list = ans.indices.numpy().tolist()
        print(ans_list)
        result = []
        for item in ans_list:
            temp_str = ix_to_tag[item]
            temp_list = temp_str.split("-")
            temp_ans = []
            for key in temp_list:
                temp_ans.append(id_to_operators[key])
            print("-".join(temp_ans))
            result.append(temp_ans)
        return result

    def statistics_operations(self):
        total_broadcast_join_count = 0
        total_sortmerge_join_count = 0
        total_union_count = 0

        for sentence, tags in training_data:
            for token in sentence:
                broadcast_join_count = token.count("B")
                total_broadcast_join_count += broadcast_join_count
                sortmerge_join_count = token.count("C")
                total_sortmerge_join_count += sortmerge_join_count
                union_count = token.count("D")
                total_union_count += union_count

        total = total_broadcast_join_count + total_sortmerge_join_count + total_union_count

        print(" total_broadcast_join_count :{} total_sortmerge_join_count:{} total_union_count:{}".format(total_broadcast_join_count,
                                                                                        total_sortmerge_join_count,
                                                                                        total_union_count))

        broadcast_join_pro = total_broadcast_join_count / total
        sortmerge_join_pro = total_sortmerge_join_count / total
        union_count_pro = total_union_count / total
        print(" broadcast_join_pro :{:.2f} sortmerge_join_pro:{:.2f} union_count_pro:{:.2f}".format(broadcast_join_pro,
                                                                                        sortmerge_join_pro,
                                                                                        union_count_pro))


#
if __name__ == '__main__':
    lstm_wrapper = LSTMWrapper()
    lstm_wrapper.train()
    # long_chain = "AfB BC CC CC CE"
    long_chain="AfC CC CC CE"
    # long_chain = "AfB BE"
    # test_seq="CB BE".split()
    result = lstm_wrapper.predict(long_chain)
    print(result)

    lstm_wrapper.statistics_operations()
