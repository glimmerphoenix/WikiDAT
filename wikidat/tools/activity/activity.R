library(Hmisc)
library(car)

stats = read.csv("data_082011.csv")

sink('results/describe.txt')
describe(stats)
sink()

# Build linear model
md = lm(log(very_active_wikipedians) ~ log(active_wikipedians), data = stats)

pdf("figs/simple_linear_model.pdf", width = 11, height = 7)
with(stats, plot(log(active_wikipedians), log(very_active_wikipedians), 
                 main = "Example of lm() with active and very active users"))
abline(coef(md), col = "navy")
with(stats, lines(lowess(log(active_wikipedians), log(very_active_wikipedians)),
                  col = "red"))
dev.off()

pdf("figs/scatterplot_car.pdf", width = 11, height = 7)
scatterplot(log(very_active_wikipedians) ~ log(active_wikipedians), 
            span = 0.6, lwd=1.5, id.n = 4, data = stats)
dev.off()

pdf("figs/boxplot_car.pdf", width = 11, height = 7)
Boxplot(~ new_fa_articles, data = stats, 
        main = "Number of new FAs in Aug. 2011")
dev.off()

pdf("figs/scatterplotMatrix.pdf", width = 11, height = 7)
scatterplotMatrix(~ log(fa_articles) + log(edits_per_month) + 
    log(other_contributors), span = 0.7, data = stats)
dev.off()

# scatter3d(log(new_articles) ~ log(fa_articles) + log(articles), 
# id.n = 3, data = stats)

# scatterplotMatrix(~ log(new_articles) + log(fa_articles) + log(articles), 
# span = 0.7, data = stats)

# symbox(){car} finding transformations
