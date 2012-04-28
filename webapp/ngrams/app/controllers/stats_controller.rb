class StatsController < ApplicationController
  def index
    search_term = ''
    if params[:index]
      search_term = params[:index][:search]
    end
    terms = search_term.split(' ')
    stats = terms.map { |t| Stat.where(:word => t).first}

    words = []
    data_wiki = []
    data_ngram = []
    legend = ['wiki', 'ngram']
    stats.each do |stat|
      if stat
        data_wiki << stat.wiki_freq
        data_ngram << stat.ngram_freq
      else
        data_wiki << 0
        data_ngram << 0
      end
    end


    if terms.count <= 0
    else
      @chart = Gchart.bar(:title => 'graph',
                        :data => [data_wiki, data_ngram],
                        :width => '600',
                        :height => '400',
                        :legend => legend,
                        :stacked => false,
                        #:orientation => 'horizontal',
                        :hAxes => {:title => 'blah'},
                        :bar_colors => 'FF0000,00FF00')
    end
  end
end
