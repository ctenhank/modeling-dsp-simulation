package kr.ac.knu.sslab.storm.examples.topology.spout;


import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

public class RandomSentenceSpout extends BaseRichSpout {
    // private static final Logger LOG =
    // LoggerFactory.getLogger(RandomSentenceSpout.class);
    static private String[] sentences_100 = new String[] {
            "Lolita, light of my life, fire of my loins. My sin, my soul. Lo-lee-ta: the tip of the tongue taking a trip of three steps down the palate to tap, at three, on the teeth. Lo. Lee. Ta.",
            "It was a queer, sultry summer, the summer they electrocuted the Rosenbergs, and I didn’t know what I was doing in New York",
            "We were somewhere around Barstow on the edge of the desert when the drugs began to take hold",
            "It was a bright cold day in April, and the clocks were striking thirteen. It was a pleasure to burn.",
            "In fairy-tales, witches always wear silly black hats and black coats, and they ride on broomsticks. But this is not a fairy-tale. This is about REAL WITCHES.                        "
    };

    static private String[] sentences_250 = new String[] {
            "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief, it was the epoch of incredulity, it was the season of Light, it was the season of Darkness, it was the spring of hope, it was the winter of despair.",
            "The only people for me are the mad ones, the ones who are mad to live, mad to talk, mad to be saved, desirous of everything at the same time, the ones who never yawn or say a commonplace thing, but burn, burn, burn like fabulous yellow roman candles exploding like spiders across the stars.",
            "I must not fear. Fear is the mind-killer. Fear is the little-death that brings total obliteration. I will face my fear. I will permit it to pass over me and through me. And when it has gone past I will turn the inner eye to see its path. Where the fear has gone there will be nothing. Only I will remain.",
            "Hello babies. Welcome to Earth. It’s hot in the summer and cold in the winter. It’s round and wet and crowded. On the outside, babies, you’ve got ahundred years here. There’s only one rule that I know of, babies: “God damn it, you’ve got to be kind.”",
            "I am an invisible man. No, I am not a spook like those who haunted Edgar Allan Poe; nor am I one of your Hollywood-movie ectoplasms. I am a man of substance, of flesh and bone, fiber and liquids—and I might even be said to possess a mind. I am invisible, understand, simply because people refuse to see me."
    };
    static private String[] sentences_500 = new String[] {
            "Gibraltar as a girl where I was a Flower of the mountain yes when I put the rose in my hair like the Andalusian girls used or shall I wear a red yes and how he kissed me under the Moorish wall and I thought well as well him as another and then I asked him with my eyes to ask again yes and then he asked me would I yes to say yes my mountain flower and first I put my arms around him yes and drew him down to me so he could feel my breasts all perfume yes and his heart was going like mad and yes I said yes I will Yes.",
            "We believe that we can change the things around us in accordance with our desires—we believe it because otherwise we can see no favourable outcome. We do not think of the outcome which generally comes to pass and is also favourable: we do not succeed in changing things in accordance with our desires, but gradually our desires change. The situation that we hoped to change because it was intolerable becomes unimportant to us. We have failed to surmount the obstacle, as we were absolutely determined to do, but life has taken us round it, led us beyond it, and then if we turn round to gaze into the distance of the past, we can barely see it, so imperceptible has it become.",
            "Atticus said to Jem one day, “I’d rather you shot at tin cans in the backyard, but I know you’ll go after birds. Shoot all the blue jays you want, if you can hit ‘em, but remember it’s a sin to kill a mockingbird.” That was the only time I ever heard Atticus say it was a sin to do something, and I asked Miss Maudie about it. “Your father’s right,” she said. “Mockingbirds don’t do one thing except make music for us to enjoy. They don’t eat up people’s gardens, don’t nest in corn cribs, they don’t do one thing but sing their hearts out for us. That’s why it’s a sin to kill a mockingbird.",
            "…I think we are well-advised to keep on nodding terms with the people we used to be, whether we find them attractive company or not. Otherwise they turn up unannounced and surprise us, come hammering on the mind’s door at 4 a.m. of a bad night and demand to know who deserted them, who betrayed them, who is going to make amends. We forget all too soon the things we thought we could never forget. We forget the loves and the betrayals alike, forget what we whispered and what we screamed, forget who we were.",
            "Why not? Because I was tired of men. Hanging in doorways, standing too close, their smell of beer or fifteen-year-old whiskey. Men who didn’t come to the emergency room with you, men who left on Christmas Eve. Men who slammed the security gates, who made you love them then changed their minds. Forests of boys, their ragged shrubs full of eyes following you, grabbing your breasts, waving their money, eyes already knocking you down, taking what they felt was theirs. (…) It was a play and I knew how it ended, I didn’t want to audition for any of the roles. It was no game, no casual thrill. It was three-bullet Russian roulette."
    };

    static private String[] sentences_1000 = new String[] {
            "Lolita, light of my life, fire of my loins. My sin, my soul. Lo-lee-ta: the tip of the tongue taking a trip of three steps down the palate to tap, at three, on the teeth. Lo. Lee. Ta. She was Lo, plain Lo, in the morning, standing four feet ten in one sock. She was Lola in slacks. She was Dolly at school. She was Dolores on the dotted line. But in my arms she was always Lolita. Did she have a precursor? She did, indeed she did. In point of fact, there might have been no Lolita at all had I not loved, one summer, an initial girl-child. In a princedom by the sea. Oh when? About as many years before Lolita was born as my age was that summer. You can always count on a murderer for a fancy prose style. Ladies and gentlemen of the jury, exhibit number one is what the seraphs, the misinformed, simple, noble-winged seraphs, envied. Look at this tangle of thorns",
            "Call me Ishmael. Some years ago — never mind how long precisely — having little or no money in my purse, and nothing particular to interest me on shore, I thought I would sail about a little and see the watery part of the world. It is a way I have of driving off the spleen, and regulating the circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing up the rear of every funeral I meet; and especially whenever my hypos get such an upper hand of me, that it requires a strong moral principle to prevent me from deliberately stepping into the street, and methodically knocking people’s hats off — then, I account it high time to get to sea as soon as I can. This is my substitute for pistol and ball. With a philosophical flourish Cato throws himself upon his sword; I quietly take to the ship. There is nothing surprising in this. If they but knew it, almost all men in their degree, some time or other, cherish very nearly the same feelings towards the ocean with me.",
            "Sometimes fate is like a small sandstorm that keeps changing directions. You change direction but the sandstorm chases you. You turn again, but the storm adjusts. Over and over you play this out, like some ominous dance with death just before dawn. Why? Because this storm isn’t something that blew in from far away, something that has nothing to do with you. This storm is you. Something inside of you. So all you can do is give in to it, step right inside the storm, closing your eyes and plugging up your ears so the sand doesn’t get in, and walk through it, step by step. There’s no sun there, no moon, no direction, no sense of time. Just fine white sand swirling up into the sky like pulverized bones. That’s the kind of sandstorm you need to imagine.And you really will have to make it through that violent, metaphysical, symbolic storm. No matter how metaphysical or symbolic",
            "It doesn’t interest me what you do for a living. I want to know what you ache for, and if you dare to dream of meeting your heart’s longing. It doesn’t interest me how old you are. I want to know if you will risk looking like a fool for love, for your dream, for the adventure of being alive. It doesn’t interest me what planets are squaring your moon. I want to know if you have touched the center of your own sorrow, if you have been opened by life’s betrayals or have become shriveled and closed from fear of further pain!I want to know if you can sit with pain, mine or your own, without moving to hide it or fade it, or fix it. I want to know if you can be with joy, mine or your own, if you can dance with wildness and let the ecstasy fill you to the tips of your fingers and toes without cautioning us to be careful, to be realistic, to remember the limitations of being human. It doesn’t interest me if the story you are telling me is true. I want to know if you can disappoint another to be true to yourself; if you can bear the accusation of betrayal and not betray your own soul; if you can be faithless and therefore trustworthy.",
            "You think because he doesn’t love you that you are worthless. You think that because he doesn’t want you anymore that he is right — that his judgement and opinion of you are correct. If he throws you out, then you are garbage. You think he belongs to you because you want to belong to him. Don’t. It’s a bad word, ‘belong.’ Especially when you put it with somebody you love. Love shouldn’t be like that. Did you ever see the way the clouds love a mountain? They circle all around it; sometimes you can’t even see the mountain for the clouds. But you know what? You go up top and what do you see? His head. The clouds never cover the head. His head pokes through, beacuse the clouds let him; they don’t wrap him up. They let him keep his head up high, free, with nothing to hide him or bind him. You can’t own a human being. You can’t lose what you don’t own. Suppose you did own him. Could you really love somebody who was absolutely nobody without you? You really want somebody like that?"
    };

    static private String[] sentences_1500 = new String[] {
            "Sometimes fate is like a small sandstorm that keeps changing directions. You change direction but the sandstorm chases you. You turn again, but the storm adjusts. Over and over you play this out, like some ominous dance with death just before dawn. Why? Because this storm isn’t something that blew in from far away, something that has nothing to do with you. This storm is you. Something inside of you. So all you can do is give in to it, step right inside the storm, closing your eyes and plugging up your ears so the sand doesn’t get in, and walk through it, step by step. There’s no sun there, no moon, no direction, no sense of time. Just fine white sand swirling up into the sky like pulverized bones. That’s the kind of sandstorm you need to imagine.And you really will have to make it through that violent, metaphysical, symbolic storm. No matter how metaphysical or symbolic it might be, make no mistake about it: it will cut through flesh like a thousand razor blades. People will bleed there, and you will bleed too. Hot, red blood. You’ll catch that blood in your hands, your own blood and the blood of others. And once the storm is over you won’t remember how you made it through, how you managed to survive. You won’t even be sure, in fact, whether the storm is really over. But one thing is certain. When you come out of the storm you won’t be the same person who walked in. That’s what this storm’s all about.",
            "You think because he doesn’t love you that you are worthless. You think that because he doesn’t want you anymore that he is right — that his judgement and opinion of you are correct. If he throws you out, then you are garbage. You think he belongs to you because you want to belong to him. Don’t. It’s a bad word, ‘belong.’ Especially when you put it with somebody you love. Love shouldn’t be like that. Did you ever see the way the clouds love a mountain? They circle all around it; sometimes you can’t even see the mountain for the clouds. But you know what? You go up top and what do you see? His head. The clouds never cover the head. His head pokes through, beacuse the clouds let him; they don’t wrap him up. They let him keep his head up high, free, with nothing to hide him or bind him. You can’t own a human being. You can’t lose what you don’t own. Suppose you did own him. Could you really love somebody who was absolutely nobody without you? You really want somebody like that? Somebody who falls apart when you walk out the door? You don’t, do you? And neither does he. You’re turning over your whole life to him. Your whole life, girl. And if it means so little to you that you can just give it away, hand it to him, then why should it mean any more to him? He can’t value you more than you value yourself.",
            "It doesn’t interest me what you do for a living. I want to know what you ache for, and if you dare to dream of meeting your heart’s longing. It doesn’t interest me how old you are. I want to know if you will risk looking like a fool for love, for your dream, for the adventure of being alive. It doesn’t interest me what planets are squaring your moon. I want to know if you have touched the center of your own sorrow, if you have been opened by life’s betrayals or have become shriveled and closed from fear of further pain!I want to know if you can sit with pain, mine or your own, without moving to hide it or fade it, or fix it. I want to know if you can be with joy, mine or your own, if you can dance with wildness and let the ecstasy fill you to the tips of your fingers and toes without cautioning us to be careful, to be realistic, to remember the limitations of being human. It doesn’t interest me if the story you are telling me is true. I want to know if you can disappoint another to be true to yourself; if you can bear the accusation of betrayal and not betray your own soul; if you can be faithless and therefore trustworthy. I want to know if you can see beauty even when it’s not pretty, every day,and if you can source your own life from its presence. I want to know if you can live with failure, yours and mine, and still stand on the edge of the lake and shout to the silver of the full moon, “Yes!” It doesn’t interest me to know where you live or how much money you have. I want to know if you can get up, after the night of grief and despair, weary and bruised to the bone, and do what needs to be done to feed the children. It doesn’t interest me who you know or how you came to be here. I want to know if you will stand in the center of the fire with me and not shrink back. It doesn’t interest me where or what or with whom you have studied. I want to know what sustains you, from the inside, when all else falls away. I want to know if you can be alone with yourself and if you truly like the company you keep in the empty moments",
    };

    private SpoutOutputCollector collector;
    private int taskId;
    private Random rand;
    private Meter sourceMeter;
    private Counter sourceCounter;
    private Histogram sourceHistogram;
    private String[] sentences;

    /**
     * 
     * @param dataSize the data size is word size. it can be one of [100,
     *                 250, 500, 1000, 1500]. The default is 100.
     */
    public RandomSentenceSpout(int dataSize) {
        switch (dataSize) {
            case 250:
                sentences = RandomSentenceSpout.sentences_250;
                break;
            case 500:
                sentences = RandomSentenceSpout.sentences_500;
                break;
            case 1000:
                sentences = RandomSentenceSpout.sentences_1000;
                break;
            case 1500:
                sentences = RandomSentenceSpout.sentences_1500;
                break;
            default:
                sentences = RandomSentenceSpout.sentences_100;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.taskId = context.getThisTaskId();
        this.rand = new Random();
        this.collector = collector;
        //this.sourceTimer = context.registerTimer("wc-randomSetenceTimer-" + +Integer.toString(this.taskId));
        this.sourceMeter = context.registerMeter("wc-randomSentenceMeter-" + Integer.toString(this.taskId));
        this.sourceCounter = context.registerCounter("wc-randomSentenceCounter-" + Integer.toString(this.taskId));
        this.sourceHistogram = context.registerHistogram("wc-randomSentenceHistogram-" + Integer.toString(this.taskId));
        //context.getResource(name)
    }

    @Override
    public void nextTuple() {
        Long currentTimeNano = System.nanoTime();
        final String sentence = sentences[rand.nextInt(sentences.length)];
        // LOG.debug("Emitting tuple: {} {}", sentence);
        collector.emit(new Values(sentence, System.nanoTime()));

        Long endTimeNano = System.nanoTime();
        this.sourceMeter.mark();
        this.sourceCounter.inc();
        this.sourceHistogram.update(endTimeNano - currentTimeNano);
    }

    protected String sentence(String input) {
        return input;
    }

    @Override
    public void ack(Object id) {

    }

    @Override
    public void fail(Object id) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words", "timestamp"));
    }
}
