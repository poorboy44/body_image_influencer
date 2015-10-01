# body_image_influencer

Calc the body image propagation index:
- for each user in the set, calc pos/neg sentiment score
- find "influencers"; these are users who tweeted and then a response from one of their followers (also in our data set)
- calc the pos-->pos ; neg--> neg ; pos--> neg; neg --> pos transition matrices
