library(ineq)
load("revisions.RData")

revisions_de = subset(revisions, lang == ' dewiki ', 
                      select = c(rev_id, rev_user))
count_revs = table(revisions_de[revisions_de$rev_user > 0, ]$rev_user)

# Plot Lorenz curve
pdf('Lorenz_curve.pdf', width = 12, height = 7)
plot(Lc(count_revs), main = 'Lorenz curve for dewiki sample')
dev.off()

sink('gini.txt')
print(Gini(count_revs))
sink()